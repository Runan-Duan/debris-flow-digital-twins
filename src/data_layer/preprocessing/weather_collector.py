from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import UniqueConstraint
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database Models
Base = declarative_base()

class WeatherData(Base):
    """
    Hourly weather data from AWEKAS station
    """
    __tablename__ = 'weather_data_hourly'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String(50), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    
    # Weather parameters
    temperature_c = Column(Float)
    humidity_percent = Column(Float)
    pressure_hpa = Column(Float)
    wind_speed_kmh = Column(Float)
    gust_speed_kmh = Column(Float)
    precipitation_mm = Column(Float)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    data_source = Column(String(50), default='AWEKAS_TABLE')
    
    # Ensure no duplicate timestamps for same station
    __table_args__ = (
        UniqueConstraint('station_id', 'timestamp', name='unique_station_timestamp'),
    )