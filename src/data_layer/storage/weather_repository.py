import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
import json

logger = logging.getLogger(__name__)


class WeatherRepository:
    """Handle database operations for weather data"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_observation(self, data: Dict[str, Any]) -> int:
        """Create weather observation record"""
        
        try:
            sql = text("""
                INSERT INTO weather_observations (
                    timestamp, location, rainfall_mm, intensity_mm_hr,
                    temperature_c, humidity_pct, wind_speed_ms, source, metadata
                )
                VALUES (
                    :timestamp,
                    ST_GeomFromText(:location_wkt, 4326),
                    :rainfall_mm, :intensity_mm_hr,
                    :temperature_c, :humidity_pct, :wind_speed_ms,
                    :source, :metadata::jsonb
                )
                RETURNING id
            """)
            
            result = self.db.execute(sql, {
                "timestamp": data["timestamp"],
                "location_wkt": f"POINT({data['longitude']} {data['latitude']})",
                "rainfall_mm": data.get("rainfall_mm"),
                "intensity_mm_hr": data.get("intensity_mm_hr"),
                "temperature_c": data.get("temperature_c"),
                "humidity_pct": data.get("humidity_pct"),
                "wind_speed_ms": data.get("wind_speed_ms"),
                "source": data["source"],
                "metadata": json.dumps(data.get("metadata", {}))
            })
            
            self.db.commit()
            obs_id = result.fetchone()[0]
            
            return obs_id
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating weather observation: {str(e)}")
            raise
    
    def get_recent_observations(
        self, 
        hours: int = 24,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get recent weather observations"""
        
        sql = text("""
            SELECT 
                id, timestamp, 
                ST_X(location) as longitude,
                ST_Y(location) as latitude,
                rainfall_mm, intensity_mm_hr,
                temperature_c, humidity_pct, wind_speed_ms,
                source, metadata
            FROM weather_observations
            WHERE timestamp >= NOW() - INTERVAL ':hours hours'
            ORDER BY timestamp DESC
            LIMIT :limit
        """)
        
        results = self.db.execute(sql, {
            "hours": hours,
            "limit": limit
        }).fetchall()
        
        return [self._row_to_dict(row) for row in results]
    
    def get_observations_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Get weather observations in time range"""
        
        sql = text("""
            SELECT 
                id, timestamp,
                ST_X(location) as longitude,
                ST_Y(location) as latitude,
                rainfall_mm, intensity_mm_hr,
                temperature_c, humidity_pct, wind_speed_ms,
                source, metadata
            FROM weather_observations
            WHERE timestamp BETWEEN :start_time AND :end_time
            ORDER BY timestamp ASC
        """)
        
        results = self.db.execute(sql, {
            "start_time": start_time,
            "end_time": end_time
        }).fetchall()
        
        return [self._row_to_dict(row) for row in results]
    
    def calculate_cumulative_rainfall(
        self,
        duration_minutes: int
    ) -> Optional[float]:
        """Calculate cumulative rainfall over duration"""
        
        sql = text("""
            SELECT COALESCE(SUM(rainfall_mm), 0) as total_rainfall
            FROM weather_observations
            WHERE timestamp >= NOW() - INTERVAL ':duration minutes'
        """)
        
        result = self.db.execute(sql, {
            "duration": duration_minutes
        }).fetchone()
        
        return result.total_rainfall if result else 0.0
    
    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert row to dictionary"""
        return {
            "id": row.id,
            "timestamp": row.timestamp,
            "latitude": row.latitude,
            "longitude": row.longitude,
            "rainfall_mm": row.rainfall_mm,
            "intensity_mm_hr": row.intensity_mm_hr,
            "temperature_c": row.temperature_c,
            "humidity_pct": row.humidity_pct,
            "wind_speed_ms": row.wind_speed_ms,
            "source": row.source,
            "metadata": row.metadata if isinstance(row.metadata, dict) else {}
        }