import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from src.data_layer.preprocessing.saga_preprocessor import SAGAPreprocessor
from config.settings import settings

from config.logging import setup_logging

logger = setup_logging()


def main():
    
    # Input paths
    dem_path = Path("data/raw/dem/merged_dsm_2022.tif")
    dtm_path = Path("data/raw/dtm/merged_dtm_2022.tif")  
    ortho_path = Path("data/raw/ortho/merged_ortho_2022.tif")
    input_dem = dtm_path  

    preprocessing_dir = settings.PROCESSED_DATA_DIR / "preprocessing"

    # Preprocessing
    # logger.info("PREPROCESSIaNG")
    
    preprocessor = SAGAPreprocessor()
    preprocessed = preprocessor.preprocess_complete_pipeline(
        input_dem=input_dem,
        output_dir=preprocessing_dir,
        method="fill"
    )
    
    logger.info(f"\nFinished! Preprocessed data: {preprocessing_dir}")


if __name__ == "__main__":
    main()