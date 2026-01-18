import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from src.data_layer.preprocessing.dem_preprocessing import DEMPreprocessor
from src.models.release_areas.release_area_detector import ReleaseAreaDetector
from config.settings import settings
from config.logging import setup_logging

logger = setup_logging()


def main():
    """Complete preprocessing and release area identification workflow"""
    
    logger.info("DEM PREPROCESSING & RELEASE AREA IDENTIFICATION")
    
    # Paths
    input_dem = Path("data/raw/dtm/merged_dtm_2022.tif")  
    dsm_path = Path("data/raw/dem/merged_dsm_2022.tif")
    dtm_path = input_dem
    ortho_path = Path("data/raw/ortho/merged_ortho_2022.tif")  
    
    output_dir = settings.PROCESSED_DATA_DIR / "preprocessing"
    release_output = settings.PROCESSED_DATA_DIR / "release_areas"
    
    if not input_dem.exists():
        logger.error(f"DEM not found: {input_dem}")
        logger.error("Please update the paths in this script")
        sys.exit(1)
    
    # Step 1: Preprocess DEM
    logger.info("STEP 1: DEM Preprocessing with WhiteboxTools")
    
    preprocessor = DEMPreprocessor()
    
    try:
        preprocessed_data = preprocessor.preprocess_pipeline(
            input_dem=input_dem,
            output_dir=output_dir,
            fill_method="fill"
        )
        
        logger.info("\nPreprocessing completed successfully")
        logger.info("\nGenerated products:")
        for key, path in preprocessed_data.items():
            logger.info(f"{key}: {path}")
    
    except Exception as e:
        logger.error(f"Preprocessing failed: {str(e)}", exc_info=True)
        sys.exit(1)
    
    # # Step 2: Identify release areas
    # logger.info("STEP 2: Release Area Identification")
    
    # detector = ReleaseAreaDetector()
    
    # # Check if DSM/DTM available
    # dsm = dsm_path if dsm_path.exists() else None
    # dtm = dtm_path if dtm_path.exists() else None
    
    # if dsm and dtm:
    #     logger.info("Using DSM/DTM for vegetation analysis")
    # else:
    #     logger.warning("DSM/DTM not available, vegetation criterion disabled")
    
    # try:
    #     release_output.mkdir(parents=True, exist_ok=True)
        
    #     # Identify using multi-criteria method
    #     release_areas = detector.identify_release_areas(
    #         preprocessed_data=preprocessed_data,
    #         dsm_path=dsm,
    #         dtm_path=dtm,
    #         method="multi_criteria",
    #         output_path=release_output / "release_areas.shp"
    #     )
        
    #     logger.info(f"\nIdentified {len(release_areas)} release areas")
    #     logger.info(f"\nStatistics:")
    #     logger.info(f"  Total area: {release_areas['area_m2'].sum():.2f} m²")
    #     logger.info(f"  Mean area: {release_areas['area_m2'].mean():.2f} m²")
    #     logger.info(f"  Min area: {release_areas['area_m2'].min():.2f} m²")
    #     logger.info(f"  Max area: {release_areas['area_m2'].max():.2f} m²")
        
    #     # Create visualization
    #     logger.info("\nGenerating visualization...")
    #     detector.visualize_criteria(
    #         preprocessed_data=preprocessed_data,
    #         output_dir=release_output
    #     )
        
    # except Exception as e:
    #     logger.error(f"Release area identification failed: {str(e)}", exc_info=True)
    #     sys.exit(1)
    
    # # Summary
    # logger.info("WORKFLOW COMPLETED SUCCESSFULLY")
    # logger.info(f"\nPreprocessed data: {output_dir}")
    # logger.info(f"Release areas: {release_output}")
    # logger.info(f"\nNext steps:")
    # logger.info(f"  1. Review release_areas.shp in QGIS")
    # logger.info(f"  2. Adjust criteria if needed")
    # logger.info(f"  3. Use release areas as input for SAGA GPP simulation")


if __name__ == "__main__":
    main()