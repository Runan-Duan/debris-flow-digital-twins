import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
import json

logger = logging.getLogger(__name__)


class SyntheticTerrainRepository:
    """Handle database operations for synthetic terrain snapshots"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_synthetic_snapshot(
        self,
        base_snapshot_id: int,
        synthetic_data: Dict[str, Any]
    ) -> int:
        """
        Create a synthetic terrain snapshot record
        
        Args:
            base_snapshot_id: ID of the base terrain this was derived from
            synthetic_data: Data from synthetic updater
            
        Returns:
            ID of created synthetic snapshot
        """
        try:
            # Get base snapshot extent
            base_sql = text("""
                SELECT extent, epsg_code, resolution_m
                FROM terrain_snapshots
                WHERE id = :id
            """)
            base = self.db.execute(base_sql, {"id": base_snapshot_id}).fetchone()
            
            if not base:
                raise ValueError(f"Base snapshot {base_snapshot_id} not found")
            
            # Insert synthetic snapshot
            sql = text("""
                INSERT INTO terrain_snapshots (
                    timestamp, version_name, dem_path, dtm_path, ortho_path,
                    resolution_m, epsg_code, extent, source, metadata
                )
                VALUES (
                    NOW(),
                    :version_name,
                    :dem_path,
                    NULL,
                    NULL,
                    :resolution_m,
                    :epsg_code,
                    :extent,
                    'synthetic',
                    :metadata::jsonb
                )
                RETURNING id
            """)
            
            result = self.db.execute(sql, {
                "version_name": synthetic_data["version_name"],
                "dem_path": synthetic_data["output_path"],
                "resolution_m": base.resolution_m,
                "epsg_code": base.epsg_code,
                "extent": base.extent,
                "metadata": json.dumps({
                    "base_snapshot_id": base_snapshot_id,
                    "modification_type": synthetic_data["modification_type"],
                    "statistics": synthetic_data["statistics"],
                    "zones": synthetic_data.get("zones", []),
                    "is_synthetic": True
                })
            })
            
            self.db.commit()
            snapshot_id = result.fetchone()[0]
            
            logger.info(f"Created synthetic terrain snapshot with ID: {snapshot_id}")
            return snapshot_id
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating synthetic snapshot: {str(e)}")
            raise
    
    def get_synthetic_snapshots(
        self,
        base_snapshot_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get all synthetic terrain snapshots, optionally filtered by base"""
        
        if base_snapshot_id:
            sql = text("""
                SELECT 
                    id, timestamp, version_name, dem_path,
                    resolution_m, epsg_code, metadata, created_at
                FROM terrain_snapshots
                WHERE source = 'synthetic'
                  AND metadata->>'base_snapshot_id' = :base_id
                ORDER BY created_at DESC
            """)
            results = self.db.execute(sql, {"base_id": str(base_snapshot_id)}).fetchall()
        else:
            sql = text("""
                SELECT 
                    id, timestamp, version_name, dem_path,
                    resolution_m, epsg_code, metadata, created_at
                FROM terrain_snapshots
                WHERE source = 'synthetic'
                ORDER BY created_at DESC
            """)
            results = self.db.execute(sql).fetchall()
        
        return [self._row_to_dict(row) for row in results]
    
    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert row to dictionary"""
        return {
            "id": row.id,
            "timestamp": row.timestamp,
            "version_name": row.version_name,
            "dem_path": row.dem_path,
            "resolution_m": row.resolution_m,
            "epsg_code": row.epsg_code,
            "metadata": row.metadata if isinstance(row.metadata, dict) else {},
            "created_at": row.created_at
        }