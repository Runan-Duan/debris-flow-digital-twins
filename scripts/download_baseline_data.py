import sys
import logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_layer.ingest.terrain_ingest import TerrainIngestor
from src.data_layer.storage.terrain_repository import TerrainRepository
from config.database import SessionLocal
from config.logging import setup_logging

logger = setup_logging()


def main():
    """
    Ingest initial baseline terrain data
    """
    
    # Paths to data
    dem_path = Path("data/raw/dem/merged_dsm_2022.tif")
    dtm_path = Path("data/raw/dtm/merged_dtm_2022.tif")  
    ortho_path = Path("data/raw/ortho/merged_ortho_2022.tif")  
    
    # Check files exist
    if not dem_path.exists():
        logger.error(f"DEM file not found: {dem_path}")
        logger.error("Please update the paths in this script to point to your data")
        sys.exit(1)
    
    logger.info("Starting baseline terrain data ingestion")
    
    # Initialize ingestor
    ingestor = TerrainIngestor()
    
    # Validate DEM
    logger.info("Validating DEM...")
    is_valid, message = ingestor.validate_dem(dem_path)
    if not is_valid:
        logger.error(f"DEM validation failed: {message}")
        sys.exit(1)
    logger.info(f"{message}")
    
    # Ingest data
    try:
        result = ingestor.ingest_baseline(
            dem_path=dem_path,
            version_name="baseline_2022",
            dtm_path=dtm_path,
            ortho_path=ortho_path,
            source="initial_survey",
            metadata={
                "survey_date": "2022",  
                "provider": "https://www.tirol.gv.at/",
                "notes": "Initial baseline dataset"
            }
        )
        
        logger.info("Terrain data processed successfully")
        logger.info(f"Version: {result['version_name']}")
        logger.info(f"Resolution: {result['resolution_m']:.2f}m")
        logger.info(f"Elevation range: {result['statistics']['min_elevation']:.1f}m "
                   f"to {result['statistics']['max_elevation']:.1f}m")
        
        # Store in database
        logger.info("Storing metadata in database...")
        db = SessionLocal()
        try:
            repo = TerrainRepository(db)
            terrain_id = repo.create_snapshot(result)
            logger.info(f"Stored in database with ID: {terrain_id}")
        finally:
            db.close()
        
        logger.info("Baseline ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()