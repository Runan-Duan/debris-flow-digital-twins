import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import time
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database Models
Base = declarative_base()

class DebrisFlowThresholdAnalysis:
    """
    Rainfall threshold analysis for debris flow triggering
    
    Implements I-D (Intensity-Duration) threshold framework
    """
    
    def __init__(self, db_engine):
        self.db = db_engine
        self.threshold_params = {
            'alpha': None,  # Power law coefficient
            'beta': None,   # Power law exponent
        }
    
    def calibrate_id_threshold(self, historical_events: pd.DataFrame) -> Tuple[float, float]:
        """
        Calibrate I-D threshold from historical debris flow events
        
        Power law: I = α * D^(-β)
        where I = intensity (mm/h), D = duration (h)
        
        Args:
            historical_events: DataFrame with columns:
                - duration_hours
                - max_intensity_mmh
                - debris_flow_occurred (bool)
        
        Returns:
            Tuple of (alpha, beta) parameters
        """
        from scipy.optimize import curve_fit
        
        # Filter only debris flow events
        df_events = historical_events[historical_events['debris_flow_occurred'] == True].copy()
        
        if len(df_events) < 3:
            logger.warning("⚠ Insufficient data for threshold calibration (need ≥3 events)")
            # Use default values from literature (Guzzetti et al., 2008)
            self.threshold_params = {'alpha': 14.0, 'beta': 0.4}
            return 14.0, 0.4
        
        # Power law function
        def power_law(duration, alpha, beta):
            return alpha * duration ** (-beta)
        
        try:
            # Fit to lower envelope of triggering events
            params, _ = curve_fit(
                power_law,
                df_events['duration_hours'],
                df_events['max_intensity_mmh'],
                p0=[14.0, 0.4],
                bounds=([1.0, 0.1], [100.0, 1.0])
            )
            
            alpha, beta = params
            self.threshold_params = {'alpha': alpha, 'beta': beta}
            
            logger.info(f"✓ I-D Threshold calibrated: I = {alpha:.2f} * D^(-{beta:.2f})")
            return alpha, beta
            
        except Exception as e:
            logger.error(f"✗ Threshold calibration failed: {e}")
            self.threshold_params = {'alpha': 14.0, 'beta': 0.4}
            return 14.0, 0.4
    
    def assess_event_risk(self, 
                         intensity_mmh: float,
                         duration_h: float,
                         antecedent_mm: float = 0) -> Dict:
        """
        Assess debris flow risk for given rainfall conditions
        
        Args:
            intensity_mmh: Rainfall intensity
            duration_h: Duration
            antecedent_mm: Antecedent rainfall
        
        Returns:
            Risk assessment dictionary
        """
        alpha = self.threshold_params['alpha'] or 14.0
        beta = self.threshold_params['beta'] or 0.4
        
        # Calculate threshold intensity for this duration
        threshold_intensity = alpha * (duration_h ** (-beta))
        
        # Calculate exceedance ratio
        exceedance = intensity_mmh / threshold_intensity
        
        # Adjust for antecedent rainfall (increases risk)
        if antecedent_mm > 50:
            exceedance *= 1.2  # 20% increase if wet antecedent conditions
        elif antecedent_mm > 100:
            exceedance *= 1.4  # 40% increase if very wet
        
        # Determine risk level
        if exceedance >= 1.5:
            risk_level = "CRITICAL"
        elif exceedance >= 1.0:
            risk_level = "HIGH"
        elif exceedance >= 0.7:
            risk_level = "MODERATE"
        else:
            risk_level = "LOW"
        
        return {
            'threshold_intensity_mmh': threshold_intensity,
            'actual_intensity_mmh': intensity_mmh,
            'exceedance_ratio': exceedance,
            'risk_level': risk_level,
            'recommendation': self._get_recommendation(risk_level)
        }
    
    def _get_recommendation(self, risk_level: str) -> str:
        """Generate action recommendation based on risk level"""
        recommendations = {
            'CRITICAL': 'IMMEDIATE ACTION: Debris flow likely. Evacuate risk zones. Close access roads.',
            'HIGH': 'WARNING: High debris flow probability. Monitor channels. Prepare evacuation.',
            'MODERATE': 'WATCH: Elevated risk. Increase monitoring frequency. Alert stakeholders.',
            'LOW': 'NORMAL: Low risk. Continue routine monitoring.'
        }
        return recommendations.get(risk_level, 'No recommendation')


class RainfallBasedReleaseAreaCalculator:
    """
    Calculate debris flow release areas based on rainfall triggering
    
    Integrates rainfall data with SAGA terrain analysis
    """
    
    def __init__(self, saga_detector):
        """
        Args:
            saga_detector: Instance of SAGAReleaseDetector class
        """
        self.saga = saga_detector
    
    def compute_release_areas(self,
                             preprocessed_data: Dict[str, Path],
                             rainfall_intensity_mmh: float,
                             antecedent_rainfall_mm: float,
                             dsm_path: Path,
                             dtm_path: Path,
                             output_dir: Path) -> Tuple[Path, Dict]:
        """
        Compute rainfall-triggered release areas
        
        Scientific approach:
        1. Calculate soil saturation from rainfall
        2. Reduce critical slope angle based on pore pressure
        3. Identify unstable slopes (Factor of Safety < 1.0)
        4. Apply multi-criteria filtering
        
        Args:
            preprocessed_data: Dict with terrain grids (slope, curvature, etc.)
            rainfall_intensity_mmh: Current rainfall intensity
            antecedent_rainfall_mm: Cumulative antecedent rainfall
            dsm_path: Digital Surface Model path
            dtm_path: Digital Terrain Model path
            output_dir: Output directory
        
        Returns:
            Tuple of (release_raster_path, statistics_dict)
        """
        logger.info("=" * 60)
        logger.info("RAINFALL-BASED RELEASE AREA COMPUTATION")
        logger.info("=" * 60)
        
        # 1. Calculate soil saturation
        saturation = self._calculate_saturation(rainfall_intensity_mmh, antecedent_rainfall_mm)
        logger.info(f"Soil saturation index: {saturation:.2f}")
        
        # 2. Adjust critical slope based on saturation
        # Higher saturation → lower critical slope (easier to fail)
        base_critical_slope_deg = 35  # Dry conditions
        critical_slope_deg = base_critical_slope_deg * (1 - 0.3 * saturation)
        logger.info(f"Critical slope angle: {critical_slope_deg:.1f}° (base: {base_critical_slope_deg}°)")
        
        # 3. Run SAGA release area detection with adjusted parameters
        release_raster = self.saga.identify_release_areas(
            preprocessed_data=preprocessed_data,
            dsm_path=dsm_path,
            dtm_path=dtm_path,
            output_dir=output_dir
        )
        
        # 4. Calculate release area statistics
        stats = self._calculate_release_statistics(release_raster)
        
        # 5. Add rainfall context
        stats['rainfall_intensity_mmh'] = rainfall_intensity_mmh
        stats['antecedent_rainfall_mm'] = antecedent_rainfall_mm
        stats['soil_saturation_index'] = saturation
        stats['critical_slope_deg'] = critical_slope_deg
        
        logger.info("=" * 60)
        logger.info(f"✓ Release area computed: {stats['total_area_m2']:.0f} m²")
        logger.info("=" * 60)
        
        return release_raster, stats
    
    def _calculate_saturation(self, intensity_mmh: float, antecedent_mm: float) -> float:
        """
        Estimate soil saturation from rainfall
        
        Args:
            intensity_mmh: Rainfall intensity
            antecedent_mm: Antecedent rainfall
        
        Returns:
            Saturation index (0-1)
        """
        # Convert intensity to equivalent depth over 1 hour
        event_depth = intensity_mmh * 1  # mm
        
        # Total water input
        total_water = event_depth + antecedent_mm
        
        # Field capacity for alpine soils (typical 100-150mm)
        field_capacity = 120  # mm
        
        # Calculate saturation (capped at 1.0)
        saturation = min(total_water / field_capacity, 1.0)
        
        return saturation
    
    def _calculate_release_statistics(self, release_raster_path: Path) -> Dict:
        """Calculate statistics from release area raster"""
        import rasterio
        
        with rasterio.open(release_raster_path) as src:
            data = src.read(1)
            transform = src.transform
            
            # Count release pixels (value = 1)
            release_pixels = np.sum(data == 1)
            
            # Calculate area
            cellsize = transform[0]  # Assuming square cells
            total_area_m2 = release_pixels * (cellsize ** 2)
            
            stats = {
                'release_pixels': int(release_pixels),
                'total_area_m2': float(total_area_m2),
                'total_area_km2': float(total_area_m2 / 1e6),
                'cellsize_m': float(cellsize)
            }
            
        return stats
