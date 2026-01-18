import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from src.data_layer.preprocessing.saga_preprocessor import SAGAPreprocessor
from src.models.release_areas.saga_release_detector import SAGAReleaseDetector
from src.models.runout.saga_gpp_runner import SAGAGPPRunner
from config.settings import settings
from config.logging import setup_logging

logger = setup_logging()


def main():
    """
    1. Preprocessing (SAGA)
    2. Release area identification (SAGA)
    3. GPP simulation (SAGA)
    """
    
    logger.info("="*70)
    logger.info("COMPLETE SAGA-ONLY DEBRIS FLOW DIGITAL TWIN WORKFLOW")
    logger.info("="*70)
    
    # Input paths - UPDATE THESE!
    input_dem = Path("data/raw/dem/your_dem.tif")
    dsm_path = Path("data/raw/dsm/your_dsm.tif") if Path("data/raw/dsm/your_dsm.tif").exists() else None
    dtm_path = Path("data/raw/dtm/your_dtm.tif") if Path("data/raw/dtm/your_dtm.tif").exists() else None
    
    preprocessing_dir = settings.PROCESSED_DATA_DIR / "preprocessing"
    release_dir = settings.PROCESSED_DATA_DIR / "release_areas"
    simulation_dir = settings.PROCESSED_DATA_DIR / "simulations" / "run_001"
    
    # Step 1: Preprocessing
    logger.info("\n" + "="*70)
    logger.info("STEP 1: PREPROCESSING")
    logger.info("="*70)
    
    preprocessor = SAGAPreprocessor()
    preprocessed = preprocessor.preprocess_complete_pipeline(
        input_dem=input_dem,
        output_dir=preprocessing_dir,
        method="fill"
    )
    
    # Step 2: Release Areas
    logger.info("\n" + "="*70)
    logger.info("STEP 2: RELEASE AREA IDENTIFICATION")
    logger.info("="*70)
    
    detector = SAGAReleaseDetector()
    release_shp = detector.identify_release_areas(
        preprocessed_data=preprocessed,
        dsm_path=dsm_path,
        dtm_path=dtm_path,
        output_dir=release_dir
    )
    
    # Step 3: GPP Simulation
    logger.info("\n" + "="*70)
    logger.info("STEP 3: GPP SIMULATION")
    logger.info("="*70)
    
    gpp = SAGAGPPRunner()
    results = gpp.run_gpp_simulation(
        dem_path=preprocessed['filled_dem'],
        release_areas_shp=release_shp,
        output_dir=simulation_dir,
        friction_model=3,  # PCM
        friction_angle=32.0,
        friction_mu=0.1,
        mass_to_drag=300.0,
        iterations=1000
    )
    
    # Summary
    logger.info("\n" + "="*70)
    logger.info("✓✓✓ COMPLETE WORKFLOW FINISHED ✓✓✓")
    logger.info("="*70)
    logger.info(f"\nPreprocessed data: {preprocessing_dir}")
    logger.info(f"Release areas: {release_shp}")
    logger.info(f"Simulation results: {simulation_dir}")
    logger.info("\nNext steps:")
    logger.info("  1. Open results in QGIS")
    logger.info("  2. Review release areas and adjust criteria if needed")
    logger.info("  3. Calibrate GPP friction parameters if you have historical events")
    logger.info("  4. Create synthetic terrain scenarios")
    logger.info("  5. Set up automated monitoring")


if __name__ == "__main__":
    main()