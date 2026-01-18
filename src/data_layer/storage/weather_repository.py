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

class RainfallDatabase:
    """
    Database manager for rainfall data storage and retrieval
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize database connection
        
        Args:
            connection_string: PostgreSQL connection string
                e.g., "postgresql://user:password@localhost:5432/debris_flow_dt"
        """
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        logger.info("✓ Database connection established")
    
    def store_rainfall_data(self, data: Dict) -> bool:
        """
        Store scraped rainfall data to database
        
        Args:
            data: Dictionary with rainfall parameters
        
        Returns:
            True if successful
        """
        try:
            record = RainfallData(**data)
            self.session.add(record)
            self.session.commit()
            logger.info(f"✓ Stored rainfall data: {data['timestamp']}")
            return True
        except Exception as e:
            self.session.rollback()
            logger.error(f"✗ Error storing data: {e}")
            return False
    
    def get_rainfall_timeseries(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Retrieve rainfall time series from database
        
        Args:
            start_date: Start of time period
            end_date: End of time period
        
        Returns:
            DataFrame with rainfall data
        """
        query = f"""
            SELECT timestamp, rainfall_mm, rainfall_rate_mmh, temperature_c
            FROM rainfall_data
            WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'
            ORDER BY timestamp
        """
        df = pd.read_sql(query, self.engine)
        logger.info(f"✓ Retrieved {len(df)} rainfall records")
        return df
    
    def calculate_antecedent_rainfall(self, target_date: datetime, days: int = 7) -> float:
        """
        Calculate cumulative rainfall before target date
        
        Args:
            target_date: Reference date
            days: Number of days to look back
        
        Returns:
            Cumulative rainfall in mm
        """
        start = target_date - timedelta(days=days)
        query = f"""
            SELECT SUM(rainfall_mm) as total
            FROM rainfall_data
            WHERE timestamp >= '{start}' AND timestamp < '{target_date}'
        """
        result = pd.read_sql(query, self.engine)
        total = result.iloc[0]['total'] if not result.empty else 0.0
        return float(total) if total else 0.0


class RainfallEventDetector:
    """
    Detect and characterize rainfall events for debris flow analysis
    
    Scientific basis:
    - Debris flows are triggered by rainfall intensity + antecedent moisture
    - I-D (Intensity-Duration) thresholds are empirically derived
    - Effective antecedent rainfall uses exponential decay weighting
    """
    
    def __init__(self, db_engine):
        self.db = db_engine
        self.min_inter_event_hours = 6  # Separate events by 6-hour dry period
        self.min_rainfall_mm = 5  # Minimum to consider as event
    
    def detect_events(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Identify discrete rainfall events from continuous data
        
        Args:
            start_date: Start of analysis period
            end_date: End of analysis period
        
        Returns:
            List of rainfall event dictionaries
        """
        # Get time series
        query = f"""
            SELECT timestamp, rainfall_mm, rainfall_rate_mmh
            FROM rainfall_data
            WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'
            ORDER BY timestamp
        """
        df = pd.read_sql(query, self.db)
        
        if df.empty:
            return []
        
        # Identify event boundaries
        df['time_diff_hours'] = df['timestamp'].diff().dt.total_seconds() / 3600
        df['new_event'] = (df['time_diff_hours'] > self.min_inter_event_hours) | df['time_diff_hours'].isna()
        df['event_id'] = df['new_event'].cumsum()
        
        # Aggregate events
        events = []
        for event_id, group in df.groupby('event_id'):
            if group['rainfall_mm'].sum() < self.min_rainfall_mm:
                continue  # Skip tiny events
            
            event = {
                'start_time': group['timestamp'].min(),
                'end_time': group['timestamp'].max(),
                'duration_hours': (group['timestamp'].max() - group['timestamp'].min()).total_seconds() / 3600,
                'total_rainfall_mm': group['rainfall_mm'].sum(),
                'max_intensity_mmh': group['rainfall_rate_mmh'].max(),
                'mean_intensity_mmh': group['rainfall_rate_mmh'].mean(),
            }
            
            # Calculate peak 15-minute intensity (if data resolution allows)
            event['peak_15min_intensity_mmh'] = self._calculate_peak_intensity(
                group, window_minutes=15
            )
            
            events.append(event)
        
        logger.info(f"✓ Detected {len(events)} rainfall events")
        return events
    
    def _calculate_peak_intensity(self, df: pd.DataFrame, window_minutes: int = 15) -> float:
        """Calculate maximum rainfall intensity over moving window"""
        try:
            df_sorted = df.sort_values('timestamp')
            df_sorted.set_index('timestamp', inplace=True)
            rolling_sum = df_sorted['rainfall_mm'].rolling(f'{window_minutes}min').sum()
            peak_mm = rolling_sum.max()
            # Convert to mm/hour
            return (peak_mm / window_minutes) * 60
        except:
            return df['rainfall_rate_mmh'].max()
    
    def calculate_effective_antecedent_rainfall(self, 
                                                target_date: datetime,
                                                days_back: int = 14,
                                                decay_factor: float = 0.84) -> float:
        """
        Calculate effective antecedent rainfall with exponential decay
        
        Based on Chleborad et al. (2006) - recent rainfall weighted more heavily
        
        Args:
            target_date: Reference date
            days_back: How many days to include
            decay_factor: Exponential decay (0.84 common for debris flows)
        
        Returns:
            Effective antecedent rainfall in mm
        """
        start_date = target_date - timedelta(days=days_back)
        
        query = f"""
            SELECT DATE(timestamp) as date, SUM(rainfall_mm) as daily_rain
            FROM rainfall_data
            WHERE timestamp >= '{start_date}' AND timestamp < '{target_date}'
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
        """
        df = pd.read_sql(query, self.db)
        
        if df.empty:
            return 0.0
        
        # Apply exponential decay weights
        df['days_ago'] = (target_date.date() - df['date']).dt.days
        df['weight'] = decay_factor ** df['days_ago']
        df['weighted_rain'] = df['daily_rain'] * df['weight']
        
        effective_rainfall = df['weighted_rain'].sum()
        return effective_rainfall
    
    def calculate_soil_saturation_index(self, 
                                       total_rainfall_mm: float,
                                       antecedent_mm: float,
                                       field_capacity_mm: float = 150) -> float:
        """
        Estimate soil saturation (0-1 scale)
        
        Args:
            total_rainfall_mm: Event rainfall
            antecedent_mm: Antecedent rainfall
            field_capacity_mm: Soil water holding capacity (typical 100-200mm)
        
        Returns:
            Saturation index 0-1
        """
        total_water = total_rainfall_mm + antecedent_mm
        saturation = min(total_water / field_capacity_mm, 1.0)
        return saturation

