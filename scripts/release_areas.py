import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
import json
from src.models.release_areas.saga_release_detector import SAGAReleaseDetector
from config.settings import settings

from config.logging import setup_logging

logger = setup_logging()


def main():
    
    # Input paths
    dsm_path = Path("data/raw/dem/merged_dsm_2022.tif")
    dtm_path = Path("data/raw/dtm/merged_dtm_2022.tif")  
    ortho_path = Path("data/raw/ortho/merged_ortho_2022.tif")
    input_dem = dtm_path  
    
    preprocessing_dir = settings.PROCESSED_DATA_DIR / "preprocessing"
    release_dir = settings.PROCESSED_DATA_DIR / "release_areas"

    with open(preprocessing_dir / "preprocessing_metadata.json", 'r') as f:
        metadata = json.load(f)
        preprocessed = metadata["outputs"]

    # Release area
    # logger.info("RELEASE AREA IDENTIFICATION")
    
    detector = SAGAReleaseDetector()
    release_shp = detector.identify_release_areas(
        preprocessed_data=preprocessed,
        dsm_path=dsm_path,
        dtm_path=dtm_path,
        output_dir=release_dir
    )
    
    # logger.info(f"\nFinished! Released areas data: {release_dir}")


if __name__ == "__main__":
    main()