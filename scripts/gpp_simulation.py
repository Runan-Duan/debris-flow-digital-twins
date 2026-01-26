import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
import geopandas as gpd
from src.models.runout.saga_gpp_runner import SAGAGPPRunner
# from src.data_layer.storage.simulation_repository import SimulationRepository
from config.database import SessionLocal
from config.settings import settings

from config.logging import setup_logging

logger = setup_logging()


def main():
    """Run SAGA GPP simulation"""
    
    logger.info("SAGA GIS GPP DEBRIS FLOW SIMULATION")
    
    # Input paths
    filled_dem = settings.PROCESSED_DATA_DIR / "preprocessing" / "dem_filled.sdat"
    release_areas_shp = settings.PROCESSED_DATA_DIR / "release_areas" / "release_polygon_3.shp"
    output_dir = settings.PROCESSED_DATA_DIR / "simulations" / "sim_003"
    
    # Verify inputs exist
    if not filled_dem.exists():
        logger.error(f"DEM not found: {filled_dem}")
        logger.error("Please provide filled DEM first")
        sys.exit(1)
    
    if not release_areas_shp.exists():
        logger.error(f"Release areas not found: {release_areas_shp}")
        logger.error("Please provide release area first")
        sys.exit(1)
    
    # Load release areas
    logger.info(f"\nLoading release areas from: {release_areas_shp}")
    
    # Initialize SAGA GPP
    gpp = SAGAGPPRunner()
    
    # Run simulation
    logger.info("Running GPP Simulation")
    
    try:
        results = gpp.run_gpp_simulation(
            dem_path=filled_dem,
            release_areas_shp=release_areas_shp,
            output_dir=output_dir,
            friction_model=5,
            friction_angle=32.0,
            friction_mu=0.1,
            mass_to_drag=200.0,
            iterations=10,
        )
        
        # Store in database
        # logger.info("\nStoring results in database...")
        # db = SessionLocal()
        # try:
        #     repo = SimulationRepository(db)
        #     sim_id = repo.create_simulation(results)
        #     logger.info(f"Stored simulation with ID: {sim_id}")
        # finally:
        #     db.close()
        
    except Exception as e:
        logger.error(f"Simulation failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()