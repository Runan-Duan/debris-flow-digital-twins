import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy.ext.declarative import declarative_base
import re
import logging


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database Models
Base = declarative_base()

class AWEKASTableScraper:
    """
    Scraper for AWEKAS table page
    """
    
    def __init__(self, station_id: str = "34362"):
        self.station_id = station_id
        self.base_url = "https://stationsweb.awekas.at"
        self.table_url = f"{self.base_url}/en/{self.station_id}/index-tab"
        
        # Setup HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        
        logger.info(f" Scraper initialized for station {station_id}")
    
    def fetch_table_page(self) -> BeautifulSoup:
        """Fetch the table page HTML"""
        try:
            logger.info(f" Fetching: {self.table_url}")
            response = self.session.get(self.table_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            logger.info(f" Page fetched ({len(response.content)} bytes)")
            
            return soup
            
        except Exception as e:
            logger.error(f" Error fetching page: {e}")
            return None
    
    def extract_date_from_page(self, soup: BeautifulSoup) -> datetime.date:
        """Extract date from the date card"""
        try:
            date_card = soup.find('div', class_='date card')
            if date_card:
                date_text_elem = date_card.find('ion-text')
                if date_text_elem:
                    date_text = date_text_elem.get_text(strip=True)
                    logger.info(f" Date: {date_text}")
                    
                    # Parse "January 18, 2026"
                    date_obj = datetime.strptime(date_text, "%B %d, %Y")
                    return date_obj.date()
            
            logger.warning(" Using today's date as fallback")
            return datetime.now().date()
            
        except Exception as e:
            logger.error(f" Date extraction error: {e}")
            return datetime.now().date()
    
    def parse_table_data(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse the weather data table"""
        try:
            table_date = self.extract_date_from_page(soup)
            
            # Find table
            table = soup.find('table')
            if not table:
                logger.error(" No table found")
                return pd.DataFrame()
            
            # Extract headers
            headers = []
            thead = table.find('thead')
            if thead:
                header_row = thead.find('tr')
                headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
                logger.info(f" Headers: {headers}")
            
            # Extract rows
            data_rows = []
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                logger.info(f"Found {len(rows)} rows")
                
                for row in rows:
                    cells = row.find_all('td')
                    if cells:
                        row_data = [cell.get_text(strip=True) for cell in cells]
                        data_rows.append(row_data)
            
            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=headers)
            df = self._clean_dataframe(df, table_date)
            
            logger.info(f" Parsed {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f" Parsing error: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def _clean_dataframe(self, df: pd.DataFrame, table_date: datetime.date) -> pd.DataFrame:
        """Clean and convert DataFrame"""
        if df.empty:
            return df
        
        # Create timestamps
        def create_timestamp(time_str):
            try:
                time_obj = datetime.strptime(time_str, "%H:%M").time()
                return datetime.combine(table_date, time_obj)
            except:
                return None
        
        df['timestamp'] = df['Time'].apply(create_timestamp)
        
        # Clean numbers
        def clean_number(value):
            try:
                clean = re.sub(r'[°CkKmMhHpPaA%]', '', value)
                clean = clean.strip()
                return float(clean)
            except:
                return None
        
        # Map columns
        column_mapping = {
            'Temperature': 'temperature_c',
            'Humidity': 'humidity_percent',
            'Air pressure': 'pressure_hpa',
            'Wind speed': 'wind_speed_kmh',
            'Gust speed': 'gust_speed_kmh',
            'Precipitation': 'precipitation_mm'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col].apply(clean_number)
        
        df['station_id'] = self.station_id
        
        # Select final columns
        final_columns = ['station_id', 'timestamp', 'temperature_c', 'humidity_percent',
                        'pressure_hpa', 'wind_speed_kmh', 'gust_speed_kmh', 
                        'precipitation_mm']
        
        df = df[final_columns]
        df = df.dropna(subset=['timestamp'])
        
        return df
    
    def scrape_current_day(self) -> pd.DataFrame:
        """Scrape current day's data"""
        soup = self.fetch_table_page()
        if soup:
            return self.parse_table_data(soup)
        return pd.DataFrame()