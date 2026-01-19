import psycopg2
from datetime import datetime, timedelta
from config.database import load_config


class WeatherRepository:
    
    def __init__(self, config=None):
        self.config = config or load_config()
    
    def create_table(self):
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data_hourly (
                        id SERIAL PRIMARY KEY,
                        station_id VARCHAR(50) DEFAULT '34362',
                        timestamp TIMESTAMP UNIQUE NOT NULL,
                        precipitation_mm FLOAT,
                        temperature_c FLOAT,
                        humidity_percent FLOAT,
                        pressure_hpa FLOAT,
                        wind_kmh FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_weather_timestamp 
                    ON weather_data_hourly(timestamp DESC)
                """)
                conn.commit()
    
    def insert_batch(self, weather_data_list):
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cur:
                for data in weather_data_list:
                    cur.execute("""
                        INSERT INTO weather_data_hourly 
                        (timestamp, precipitation_mm, temperature_c, humidity_percent, pressure_hpa, wind_kmh)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (timestamp) DO NOTHING
                    """, (
                        data['timestamp'],
                        data['precipitation_mm'],
                        data['temperature_c'],
                        data['humidity_percent'],
                        data.get('pressure_hpa', 0),
                        data['wind_kmh']
                    ))
                conn.commit()
    
    def get_recent_data(self, hours=24):
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT timestamp, precipitation_mm, temperature_c, humidity_percent
                    FROM weather_data_hourly
                    WHERE timestamp >= %s
                    ORDER BY timestamp DESC
                """, (datetime.now() - timedelta(hours=hours),))
                
                rows = cur.fetchall()
                return [{
                    'timestamp': r[0],
                    'precipitation_mm': r[1],
                    'temperature_c': r[2],
                    'humidity_percent': r[3]
                } for r in rows]
    
    def get_antecedent_rainfall(self, target_date=None, days_back=7):
        if target_date is None:
            target_date = datetime.now()
        
        start_date = target_date - timedelta(days=days_back)
        
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT SUM(precipitation_mm)
                    FROM weather_data_hourly
                    WHERE timestamp >= %s AND timestamp < %s
                """, (start_date, target_date))
                
                result = cur.fetchone()[0]
                return float(result) if result else 0.0
    
    def get_effective_antecedent_rainfall(self, target_date=None, days_back=14, decay=0.84):
        if target_date is None:
            target_date = datetime.now()
        
        start_date = target_date - timedelta(days=days_back)
        
        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT timestamp, precipitation_mm
                    FROM weather_data_hourly
                    WHERE timestamp >= %s AND timestamp < %s
                    ORDER BY timestamp DESC
                """, (start_date, target_date))
                
                rows = cur.fetchall()
                
                rain_eff = 0.0
                for row in rows:
                    days_ago = (target_date.date() - row[0].date()).days
                    rain_eff += row[1] * (decay ** days_ago)
                
                return rain_eff
    
    def get_current_event_metrics(self, target_date=None):
        if target_date is None:
            target_date = datetime.now()

        lookback = target_date - timedelta(hours=48)

        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT timestamp, precipitation_mm
                    FROM weather_data_hourly
                    WHERE timestamp >= %s AND timestamp <= %s
                    ORDER BY timestamp DESC
                """, (lookback, target_date))

                rows = cur.fetchall()

                if not rows:
                    return {
                        'max_intensity_mmh': 0.0,
                        'duration_h': 0,
                        'total_mm': 0.0
                    }

                rain_values = [r[1] for r in rows if r[1] and r[1] > 0]

                if not rain_values:
                    return {
                        'max_intensity_mmh': 0.0,
                        'duration_h': 0,
                        'total_mm': 0.0
                    }

                max_intensity = max(rain_values)

                duration_h = 0
                for ts, precip in rows:
                    if precip and precip > 0:
                        duration_h += 1
                    else:
                        break

                total_event = sum(rain_values)

                return {
                    'max_intensity_mmh': max_intensity,
                    'duration_h': duration_h,
                    'total_mm': total_event
                }
