from datetime import datetime, timedelta
from sqlalchemy import Column, Integer, Float, String, DateTime, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database Models
Base = declarative_base()

class RainfallData(Base):
    """Raw rainfall measurements from AWEKAS station"""
    __tablename__ = 'rainfall_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String(50), index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # Rainfall measurements
    rainfall_mm = Column(Float)  # Current rainfall in mm
    rainfall_rate_mmh = Column(Float)  # Rainfall rate mm/hour
    rainfall_daily_mm = Column(Float)  # Daily accumulation
    rainfall_monthly_mm = Column(Float)  # Monthly accumulation
    
    # Additional weather parameters
    temperature_c = Column(Float)
    humidity_percent = Column(Float)
    pressure_hpa = Column(Float)
    wind_speed_kmh = Column(Float)
    wind_direction_deg = Column(Float)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    data_source = Column(String(50), default='AWEKAS')

class RainfallEvent(Base):
    """Processed rainfall events for debris flow analysis"""
    __tablename__ = 'rainfall_events'
    
    event_id = Column(Integer, primary_key=True, autoincrement=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    
    # Event characteristics
    duration_hours = Column(Float)
    total_rainfall_mm = Column(Float)
    max_intensity_mmh = Column(Float)
    mean_intensity_mmh = Column(Float)
    peak_15min_intensity_mmh = Column(Float)  # Critical for debris flow
    
    # Antecedent conditions (KEY for debris flow triggering)
    antecedent_3day_mm = Column(Float)
    antecedent_7day_mm = Column(Float)
    antecedent_14day_mm = Column(Float)
    antecedent_30day_mm = Column(Float)
    
    # Effective antecedent rainfall (weighted)
    effective_antecedent_mm = Column(Float)
    
    # Soil moisture proxy
    soil_saturation_index = Column(Float)  # 0-1 scale
    
    # Debris flow occurrence flag
    debris_flow_occurred = Column(Boolean, default=False)
    debris_flow_magnitude = Column(String(20))  # small, medium, large
    
    # Analysis results
    threshold_exceedance = Column(Float)  # Ratio to I-D threshold
    risk_level = Column(String(20))  # LOW, MODERATE, HIGH, CRITICAL
    
    created_at = Column(DateTime, default=datetime.utcnow)
    notes = Column(String(500))