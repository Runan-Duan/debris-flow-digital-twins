from pydantic_settings import BaseSettings
from pathlib import Path
from typing import List


class Settings(BaseSettings):
    """Application configuration settings"""
    
    # Database
    DATABASE_URL: str
    
    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_RELOAD: bool = True
    SECRET_KEY: str
    
    # Directories
    DATA_DIR: Path = Path("./data")
    RAW_DATA_DIR: Path = Path("./data/raw")
    PROCESSED_DATA_DIR: Path = Path("./data/processed")
    EXTERNAL_DATA_DIR: Path = Path("./data/external")
    
    # Weather
    OPENWEATHER_API_KEY: str
    WEATHER_LOCATION_LAT: float
    WEATHER_LOCATION_LON: float
    WEATHER_UPDATE_INTERVAL: int = 1800
    
    # Sentinel-2
    COPERNICUS_USERNAME: str
    COPERNICUS_PASSWORD: str
    SENTINEL_AOI_WKT: str
    SENTINEL_CHECK_INTERVAL: int = 86400
    
    # DEM
    DEM_RESOLUTION: float = 1.0
    DEM_EPSG: int = 32632
    CHANGE_DETECTION_THRESHOLD: float = 0.5
    
    # Flow-R
    FLOWR_PATH: Path
    FLOWR_FRICTION_ANGLE: float = 32.0
    FLOWR_MIN_VELOCITY: float = 0.1
    
    # Rainfall Threshold
    THRESHOLD_ALPHA: float = 20.0
    THRESHOLD_BETA: float = 0.5
    MIN_TRIGGER_DURATION: int = 60
    
    # Risk
    RISK_LOW_THRESHOLD: float = 0.3
    RISK_MEDIUM_THRESHOLD: float = 0.5
    RISK_HIGH_THRESHOLD: float = 0.7
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: Path = Path("./logs/app.log")
    
    # CORS
    ALLOWED_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:5173"]
    
    class Config:
        env_file = ".env"
        case_sensitive = True
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Create directories if they don't exist
        self.DATA_DIR.mkdir(exist_ok=True)
        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.EXTERNAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.LOG_FILE.parent.mkdir(exist_ok=True)


# Singleton instance
settings = Settings()