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

class AWEKASScraper:
    """
    Web scraper for AWEKAS weather station data
    
    AWEKAS displays data dynamically, so we'll scrape the current readings
    and build historical database through regular polling
    """
    
    def __init__(self, station_id: str = "34362", base_url: str = "https://stationsweb.awekas.at"):
        self.station_id = station_id
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def scrape_current_data(self) -> Dict:
        """
        Scrape current weather data from AWEKAS station page
        
        Returns:
            Dictionary with current weather parameters
        """
        url = f"{self.base_url}/en/{self.station_id}/home"
        
        try:
            logger.info(f"Fetching data from: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract data from the page structure
            # Note: AWEKAS structure may vary - inspect the page to find exact selectors
            data = {
                'timestamp': datetime.utcnow(),
                'station_id': self.station_id,
                'rainfall_mm': self._extract_value(soup, 'rainfall', 'rain'),
                'rainfall_rate_mmh': self._extract_value(soup, 'rain rate', 'rainrate'),
                'rainfall_daily_mm': self._extract_value(soup, 'rain today', 'dailyrain'),
                'temperature_c': self._extract_value(soup, 'temperature', 'temp'),
                'humidity_percent': self._extract_value(soup, 'humidity', 'hum'),
                'pressure_hpa': self._extract_value(soup, 'pressure', 'baro'),
                'wind_speed_kmh': self._extract_value(soup, 'wind', 'windspeed'),
                'wind_direction_deg': self._extract_value(soup, 'wind direction', 'winddir'),
            }
            
            logger.info(f"Data scraped: Rainfall={data['rainfall_mm']}mm, Temp={data['temperature_c']}°C")
            return data
            
        except Exception as e:
            logger.error(f"Error scraping AWEKAS: {e}")
            return None
    
    def _extract_value(self, soup: BeautifulSoup, label: str, alt_class: str = None) -> Optional[float]:
        """
        Extract numeric value from HTML based on label or class
        
        Args:
            soup: BeautifulSoup object
            label: Text label to search for
            alt_class: Alternative CSS class name
        
        Returns:
            Extracted numeric value or None
        """
        try:
            # Method 1: Search by label text
            elements = soup.find_all(string=lambda text: text and label.lower() in text.lower())
            for elem in elements:
                parent = elem.parent
                # Look for numeric value in siblings or parent
                if parent:
                    value_elem = parent.find_next(class_=lambda x: x and ('value' in x or 'data' in x))
                    if value_elem:
                        return self._parse_number(value_elem.text)
            
            # Method 2: Search by class name
            if alt_class:
                elem = soup.find(class_=alt_class)
                if elem:
                    return self._parse_number(elem.text)
            
            # Method 3: Look in table structure (common in AWEKAS)
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    for i, cell in enumerate(cells):
                        if label.lower() in cell.text.lower() and i + 1 < len(cells):
                            return self._parse_number(cells[i + 1].text)
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not extract {label}: {e}")
            return None
    
    def _parse_number(self, text: str) -> Optional[float]:
        """Extract numeric value from text"""
        try:
            # Remove common units and symbols
            clean = text.strip().replace('mm', '').replace('°C', '').replace('%', '')
            clean = clean.replace('hPa', '').replace('km/h', '').replace(',', '.')
            
            # Extract first number found
            import re
            match = re.search(r'-?\d+\.?\d*', clean)
            if match:
                return float(match.group())
            return None
        except:
            return None
    
    def scrape_historical_csv(self, csv_url: str) -> pd.DataFrame:
        """
        If AWEKAS provides CSV export, parse it
        
        Args:
            csv_url: URL to CSV file (if available)
        
        Returns:
            DataFrame with historical data
        """
        try:
            df = pd.read_csv(csv_url, parse_dates=['timestamp'])
            logger.info(f"Loaded {len(df)} historical records from CSV")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            return pd.DataFrame()