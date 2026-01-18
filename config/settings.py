from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import List, Optional
import warnings


class Settings(BaseSettings):
    """Application configuration settings"""
    
    # Database
    DATABASE_URL: str = "sqlite:///./debris_flow.db"  
    
    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_RELOAD: bool = True
    SECRET_KEY: str = "dev-secret-key-change-in-production"
    
    # Directories
    DATA_DIR: Path = Path("./data")
    RAW_DATA_DIR: Path = Path("./data/raw")
    PROCESSED_DATA_DIR: Path = Path("./data/processed")
    EXTERNAL_DATA_DIR: Path = Path("./data/external")
    DATABASE_DIR: Path = Path("./database")
    
    # Weather
    OPENWEATHER_API_KEY: Optional[str] = None
    WEATHER_LOCATION_LAT: float = 0.0
    WEATHER_LOCATION_LON: float = 0.0
    WEATHER_UPDATE_INTERVAL: int = 1800
    
    # Sentinel-2
    COPERNICUS_USERNAME: Optional[str] = None
    COPERNICUS_PASSWORD: Optional[str] = None
    SENTINEL_AOI_WKT: str = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"  # Default placeholder
    SENTINEL_CHECK_INTERVAL: int = 86400
    
    # DEM
    DEM_RESOLUTION: float = 1.0
    DEM_EPSG: int = 4326
    CHANGE_DETECTION_THRESHOLD: float = 0.5
    
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
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",  # Ignore extra fields in .env
    )
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Validate critical settings
        self._validate_settings()
        
        # Create directories if they don't exist
        self.DATA_DIR.mkdir(exist_ok=True)
        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.EXTERNAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.LOG_FILE.parent.mkdir(exist_ok=True)
    
    def _validate_settings(self):
        """Warn about missing required configuration"""
        warnings.simplefilter("always")
        
        if not self.OPENWEATHER_API_KEY:
            warnings.warn("OPENWEATHER_API_KEY is not set. Weather features will be disabled.")
        
        if not self.COPERNICUS_USERNAME or not self.COPERNICUS_PASSWORD:
            warnings.warn("COPERNICUS credentials not set. Sentinel-2 features will be disabled.")
        
        if self.WEATHER_LOCATION_LAT == 0.0 and self.WEATHER_LOCATION_LON == 0.0:
            warnings.warn("Weather location is set to (0,0). Update WEATHER_LOCATION_LAT/LON.")


# Create singleton instance with error handling
try:
    settings = Settings()
except Exception as e:
    print(f"Error loading settings: {e}")
    print("Using default settings with warnings...")
    # Create settings with defaults only
    settings = Settings(_env_file=None)
    settings._validate_settings()