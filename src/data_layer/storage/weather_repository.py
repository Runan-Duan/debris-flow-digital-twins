import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import re
import logging
from src.data_layer.preprocessing.weather_collector import WeatherData

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database Models
Base = declarative_base()

class WeatherDatabase:
    """
    Database manager for weather data storage
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize database connection
        
        Args:
            connection_string: PostgreSQL connection string
                Example: "postgresql://user:password@localhost:5432/debris_flow_dt"
        """
        self.engine = create_engine(connection_string, echo=False)
        
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        
        # Create session
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        logger.info("✓ Database connection established")
    
    def store_dataframe(self, df: pd.DataFrame, if_exists: str = 'append') -> int:
        """
        Store DataFrame to database
        
        Args:
            df: DataFrame with weather data
            if_exists: 'append' or 'replace'
        
        Returns:
            Number of records inserted
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to store")
            return 0
        
        try:
            # Use pandas to_sql for bulk insert
            records_before = self.session.query(WeatherData).count()
            
            df.to_sql(
                'weather_data_hourly',
                self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=100
            )
            
            records_after = self.session.query(WeatherData).count()
            inserted = records_after - records_before
            
            logger.info(f"✓ Stored {inserted} new records to database")
            return inserted
            
        except Exception as e:
            logger.error(f"✗ Error storing data: {e}")
            # If it's a duplicate error, that's okay
            if 'unique constraint' in str(e).lower():
                logger.info("⚠ Some records already exist (duplicates skipped)")
                return 0
            raise
    
    def get_data_range(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Retrieve data for date range
        
        Args:
            start_date: Start datetime
            end_date: End datetime
        
        Returns:
            DataFrame with weather data
        """
        query = f"""
            SELECT * FROM weather_data_hourly
            WHERE timestamp >= '{start_date}' 
              AND timestamp <= '{end_date}'
            ORDER BY timestamp
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"✓ Retrieved {len(df)} records from database")
        
        return df
    
    def get_latest_timestamp(self, station_id: str) -> datetime:
        """
        Get the most recent timestamp for a station
        
        Args:
            station_id: Station ID
        
        Returns:
            Latest timestamp or None
        """
        query = f"""
            SELECT MAX(timestamp) as latest
            FROM weather_data_hourly
            WHERE station_id = '{station_id}'
        """
        
        result = pd.read_sql(query, self.engine)
        
        if not result.empty and result.iloc[0]['latest'] is not None:
            return result.iloc[0]['latest']
        
        return None
    
    def calculate_daily_precipitation(self, date: datetime.date) -> float:
        """
        Calculate total precipitation for a day
        
        Args:
            date: Date to calculate for
        
        Returns:
            Total precipitation in mm
        """
        start = datetime.combine(date, datetime.min.time())
        end = datetime.combine(date, datetime.max.time())
        
        query = f"""
            SELECT SUM(precipitation_mm) as total
            FROM weather_data_hourly
            WHERE timestamp >= '{start}' AND timestamp <= '{end}'
        """
        
        result = pd.read_sql(query, self.engine)
        
        if not result.empty and result.iloc[0]['total'] is not None:
            return float(result.iloc[0]['total'])
        
        return 0.0
    
    def get_statistics(self) -> dict:
        """
        Get database statistics
        
        Returns:
            Dictionary with stats
        """
        stats = {}
        
        # Total records
        stats['total_records'] = self.session.query(WeatherData).count()
        
        # Date range
        query = """
            SELECT 
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest
            FROM weather_data_hourly
        """
        result = pd.read_sql(query, self.engine)
        
        if not result.empty:
            stats['earliest_date'] = result.iloc[0]['earliest']
            stats['latest_date'] = result.iloc[0]['latest']
        
        # Records per day
        query = """
            SELECT DATE(timestamp) as date, COUNT(*) as count
            FROM weather_data_hourly
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
            LIMIT 10
        """
        stats['recent_days'] = pd.read_sql(query, self.engine)
        
        return stats
    



def insert_weather_data(config, df: pd.DataFrame) -> int:
    """
    Insert DataFrame into database
    
    Args:
        config: Database configuration
        df: DataFrame with weather data
    
    Returns:
        Number of records inserted
    """
    if df.empty:
        logger.warning("⚠ Empty DataFrame, nothing to insert")
        return 0
    
    insert_query = """
    INSERT INTO weather_data_hourly 
        (station_id, timestamp, temperature_c, humidity_percent, 
         pressure_hpa, wind_speed_kmh, gust_speed_kmh, precipitation_mm)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (station_id, timestamp) DO NOTHING;
    """
    
    inserted_count = 0
    
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # Convert DataFrame to list of tuples
                records = df.to_records(index=False)
                data_tuples = [tuple(row) for row in records]
                
                # Execute batch insert
                cur.executemany(insert_query, data_tuples)
                inserted_count = cur.rowcount
                
                conn.commit()
                logger.info(f"✓ Inserted {inserted_count} records")
                
    except (psycopg2.DatabaseError, Exception) as error:
        logger.error(f"✗ Insert error: {error}")
        return 0
    
    return inserted_count


def get_database_statistics(config):
    """Get statistics about stored data"""
    
    queries = {
        'total_records': "SELECT COUNT(*) FROM weather_data_hourly",
        'date_range': """
            SELECT 
                MIN(timestamp) as earliest, 
                MAX(timestamp) as latest,
                COUNT(DISTINCT DATE(timestamp)) as days
            FROM weather_data_hourly
        """,
        'recent_days': """
            SELECT 
                DATE(timestamp) as date,
                COUNT(*) as records,
                SUM(precipitation_mm) as total_precip_mm,
                AVG(temperature_c) as avg_temp_c
            FROM weather_data_hourly
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
            LIMIT 10
        """
    }
    
    stats = {}
    
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # Total records
                cur.execute(queries['total_records'])
                stats['total_records'] = cur.fetchone()[0]
                
                # Date range
                cur.execute(queries['date_range'])
                row = cur.fetchone()
                stats['earliest_date'] = row[0]
                stats['latest_date'] = row[1]
                stats['days_covered'] = row[2]
                
                # Recent days
                cur.execute(queries['recent_days'])
                recent = cur.fetchall()
                stats['recent_days'] = pd.DataFrame(
                    recent, 
                    columns=['date', 'records', 'total_precip_mm', 'avg_temp_c']
                )
                
    except (psycopg2.DatabaseError, Exception) as error:
        logger.error(f"✗ Statistics error: {error}")
    
    return stats


def get_precipitation_data(config, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Retrieve precipitation data for date range
    
    Args:
        config: Database configuration
        start_date: Start datetime
        end_date: End datetime
    
    Returns:
        DataFrame with timestamp and precipitation_mm
    """
    query = """
    SELECT timestamp, precipitation_mm, temperature_c, humidity_percent
    FROM weather_data_hourly
    WHERE timestamp >= %s AND timestamp <= %s
    ORDER BY timestamp
    """
    
    try:
        with psycopg2.connect(**config) as conn:
            df = pd.read_sql_query(query, conn, params=(start_date, end_date))
            logger.info(f"✓ Retrieved {len(df)} precipitation records")
            return df
    except (psycopg2.DatabaseError, Exception) as error:
        logger.error(f"✗ Query error: {error}")
        return pd.DataFrame()


def calculate_antecedent_rainfall(config, target_date: datetime, days_back: int = 7) -> float:
    """
    Calculate cumulative rainfall before target date
    
    Args:
        config: Database configuration
        target_date: Reference date
        days_back: Number of days to look back
    
    Returns:
        Total precipitation in mm
    """
    start_date = target_date - timedelta(days=days_back)
    
    query = """
    SELECT SUM(precipitation_mm) as total
    FROM weather_data_hourly
    WHERE timestamp >= %s AND timestamp < %s
    """
    
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (start_date, target_date))
                result = cur.fetchone()
                total = result[0] if result[0] is not None else 0.0
                
                logger.info(f"✓ Antecedent rainfall ({days_back} days): {total:.1f} mm")
                return float(total)
                
    except (psycopg2.DatabaseError, Exception) as error:
        logger.error(f"✗ Calculation error: {error}")
        return 0.0