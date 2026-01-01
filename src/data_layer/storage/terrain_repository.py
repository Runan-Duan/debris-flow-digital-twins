import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape, to_shape
from shapely import wkt
import json

logger = logging.getLogger(__name__)


class TerrainRepository:
    """Handle database operations for terrain snapshots"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_snapshot(self, data: Dict[str, Any]) -> int:
        """
        Create new terrain snapshot record
        
        Args:
            data: Snapshot data from ingestion
            
        Returns:
            ID of created snapshot
        """
        try:
            # Parse WKT geometry
            extent_geom = wkt.loads(data["extent_wgs84"])
            
            # Prepare SQL with proper geometry handling
            sql = text("""
                INSERT INTO terrain_snapshots (
                    timestamp, version_name, dem_path, dtm_path, ortho_path,
                    resolution_m, epsg_code, extent, source, metadata
                )
                VALUES (
                    :timestamp, :version_name, :dem_path, :dtm_path, :ortho_path,
                    :resolution_m, :epsg_code, 
                    ST_GeomFromText(:extent_wkt, 4326),
                    :source, :metadata::jsonb
                )
                RETURNING id
            """)
            
            result = self.db.execute(sql, {
                "timestamp": data["timestamp"],
                "version_name": data["version_name"],
                "dem_path": data["dem_path"],
                "dtm_path": data.get("dtm_path"),
                "ortho_path": data.get("ortho_path"),
                "resolution_m": data["resolution_m"],
                "epsg_code": data["epsg_code"],
                "extent_wkt": data["extent_wgs84"],
                "source": data["source"],
                "metadata": json.dumps({
                    **data.get("metadata", {}),
                    "statistics": data.get("statistics", {})
                })
            })
            
            self.db.commit()
            snapshot_id = result.fetchone()[0]
            
            logger.info(f"Created terrain snapshot with ID: {snapshot_id}")
            return snapshot_id
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating terrain snapshot: {str(e)}")
            raise
    
    def get_snapshot_by_id(self, snapshot_id: int) -> Optional[Dict[str, Any]]:
        """Get terrain snapshot by ID"""
        
        sql = text("""
            SELECT 
                id, timestamp, version_name, dem_path, dtm_path, ortho_path,
                resolution_m, epsg_code, 
                ST_AsText(extent) as extent_wkt,
                source, metadata, created_at
            FROM terrain_snapshots
            WHERE id = :id
        """)
        
        result = self.db.execute(sql, {"id": snapshot_id}).fetchone()
        
        if result:
            return self._row_to_dict(result)
        return None
    
    def get_snapshot_by_version(self, version_name: str) -> Optional[Dict[str, Any]]:
        """Get terrain snapshot by version name"""
        
        sql = text("""
            SELECT 
                id, timestamp, version_name, dem_path, dtm_path, ortho_path,
                resolution_m, epsg_code, 
                ST_AsText(extent) as extent_wkt,
                source, metadata, created_at
            FROM terrain_snapshots
            WHERE version_name = :version_name
        """)
        
        result = self.db.execute(sql, {"version_name": version_name}).fetchone()
        
        if result:
            return self._row_to_dict(result)
        return None
    
    def get_latest_snapshot(self) -> Optional[Dict[str, Any]]:
        """Get most recent terrain snapshot"""
        
        sql = text("""
            SELECT 
                id, timestamp, version_name, dem_path, dtm_path, ortho_path,
                resolution_m, epsg_code, 
                ST_AsText(extent) as extent_wkt,
                source, metadata, created_at
            FROM terrain_snapshots
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        
        result = self.db.execute(sql).fetchone()
        
        if result:
            return self._row_to_dict(result)
        return None
    
    def get_all_snapshots(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all terrain snapshots ordered by timestamp"""
        
        sql = text("""
            SELECT 
                id, timestamp, version_name, dem_path, dtm_path, ortho_path,
                resolution_m, epsg_code, 
                ST_AsText(extent) as extent_wkt,
                source, metadata, created_at
            FROM terrain_snapshots
            ORDER BY timestamp DESC
            LIMIT :limit
        """)
        
        results = self.db.execute(sql, {"limit": limit}).fetchall()
        
        return [self._row_to_dict(row) for row in results]
    
    def get_baseline_snapshot(self) -> Optional[Dict[str, Any]]:
        """Get the baseline (first) terrain snapshot"""
        
        sql = text("""
            SELECT 
                id, timestamp, version_name, dem_path, dtm_path, ortho_path,
                resolution_m, epsg_code, 
                ST_AsText(extent) as extent_wkt,
                source, metadata, created_at
            FROM terrain_snapshots
            WHERE source = 'baseline' OR version_name LIKE '%baseline%'
            ORDER BY timestamp ASC
            LIMIT 1
        """)
        
        result = self.db.execute(sql).fetchone()
        
        if result:
            return self._row_to_dict(result)
        return None
    
    def get_snapshots_in_timerange(
        self, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Get terrain snapshots within a time range"""
        
        sql = text("""
            SELECT 
                id, timestamp, version_name, dem_path, dtm_path, ortho_path,
                resolution_m, epsg_code, 
                ST_AsText(extent) as extent_wkt,
                source, metadata, created_at
            FROM terrain_snapshots
            WHERE timestamp BETWEEN :start_time AND :end_time
            ORDER BY timestamp ASC
        """)
        
        results = self.db.execute(sql, {
            "start_time": start_time,
            "end_time": end_time
        }).fetchall()
        
        return [self._row_to_dict(row) for row in results]
    
    def update_metadata(self, snapshot_id: int, metadata: Dict[str, Any]) -> bool:
        """Update metadata for a snapshot"""
        
        try:
            sql = text("""
                UPDATE terrain_snapshots
                SET metadata = metadata || :new_metadata::jsonb
                WHERE id = :id
            """)
            
            self.db.execute(sql, {
                "id": snapshot_id,
                "new_metadata": json.dumps(metadata)
            })
            self.db.commit()
            
            logger.info(f"Updated metadata for snapshot {snapshot_id}")
            return True
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating metadata: {str(e)}")
            return False
    
    def delete_snapshot(self, snapshot_id: int) -> bool:
        """Delete a terrain snapshot (use with caution!)"""
        
        try:
            sql = text("""
                DELETE FROM terrain_snapshots
                WHERE id = :id
            """)
            
            self.db.execute(sql, {"id": snapshot_id})
            self.db.commit()
            
            logger.warning(f"Deleted terrain snapshot {snapshot_id}")
            return True
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error deleting snapshot: {str(e)}")
            return False
    
    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert database row to dictionary"""
        return {
            "id": row.id,
            "timestamp": row.timestamp,
            "version_name": row.version_name,
            "dem_path": row.dem_path,
            "dtm_path": row.dtm_path,
            "ortho_path": row.ortho_path,
            "resolution_m": row.resolution_m,
            "epsg_code": row.epsg_code,
            "extent_wkt": row.extent_wkt,
            "source": row.source,
            "metadata": row.metadata if isinstance(row.metadata, dict) else {},
            "created_at": row.created_at
        }