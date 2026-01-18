import logging
from pathlib import Path
from typing import Dict, Any, Optional
import json

from whitebox import WhiteboxTools
from config.settings import settings

logger = logging.getLogger(__name__)


class DEMPreprocessor:
    """
    Preprocess DEMs using WhiteboxTools for hydrological correctness
    """

    def __init__(self):
        self.wbt = WhiteboxTools()
        self.wbt.verbose = True

    def preprocess_pipeline(
        self,
        input_dem: Path,
        output_dir: Path,
        fill_method: str = "breach"   # "breach" or "fill"
    ) -> Dict[str, Path]:

        logger.info(f"Starting DEM preprocessing pipeline: {input_dem}")
        logger.info(f"Fill method: {fill_method}")

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        outputs: Dict[str, Path] = {}

        # Fill / breach depressions
        if fill_method == "breach":
            outputs["filled_dem"] = output_dir / "dem_breached.tif"
            self.wbt.breach_depressions(
                dem=str(input_dem),
                output=str(outputs["filled_dem"]),
                flat_increment=0.001,
            )
        else:
            outputs["filled_dem"] = output_dir / "dem_filled.tif"
            self.wbt.fill_depressions(
                dem=str(input_dem),
                output=str(outputs["filled_dem"]),
                fix_flats=True,
            )

        # Slope
        outputs["slope"] = output_dir / "slope.tif"
        self.wbt.slope(
            dem=str(outputs["filled_dem"]),
            output=str(outputs["slope"]),
            units="degrees",
        )

        # Aspect
        outputs["aspect"] = output_dir / "aspect.tif"
        self.wbt.aspect(
            dem=str(outputs["filled_dem"]),
            output=str(outputs["aspect"]),
        )

        # Plan curvature
        outputs["plan_curvature"] = output_dir / "plan_curvature.tif"
        self.wbt.plan_curvature(
            dem=str(outputs["filled_dem"]),
            output=str(outputs["plan_curvature"]),
        )

        # Profile curvature
        outputs["profile_curvature"] = output_dir / "profile_curvature.tif"
        self.wbt.profile_curvature(
            dem=str(outputs["filled_dem"]),
            output=str(outputs["profile_curvature"]),
        )

        # Flow direction (D8)
        outputs["flow_dir_d8"] = output_dir / "flow_dir_d8.tif"
        self.wbt.d8_pointer(
            dem=str(outputs["filled_dem"]),
            output=str(outputs["flow_dir_d8"]),
        )

        # Flow accumulation (D8)
        outputs["flow_accum_d8"] = output_dir / "flow_accum_d8.tif"
        self.wbt.d8_flow_accumulation(
            input=str(outputs["filled_dem"]),
            output=str(outputs["flow_accum_d8"]),
            out_type="cells",
        )

        # Flow direction (D∞)
        outputs["flow_dir_dinf"] = output_dir / "flow_dir_dinf.tif"
        self.wbt.d_inf_pointer(
            dem=str(outputs["filled_dem"]),
            output=str(outputs["flow_dir_dinf"]),
        )

        # Flow accumulation (D∞)
        outputs["flow_accum_dinf"] = output_dir / "flow_accum_dinf.tif"
        self.wbt.d_inf_flow_accumulation(
            input=str(outputs["filled_dem"]),
            output=str(outputs["flow_accum_dinf"]),
            out_type="specific contributing area",
        )

        # TRI
        outputs["tri"] = output_dir / "tri.tif"
        self.wbt.ruggedness_index(
            dem=str(outputs["filled_dem"]),
            output=str(outputs["tri"]),
        )

        # Wetness index
        outputs["twi"] = output_dir / "twi.tif"
        self.wbt.wetness_index(
            sca=str(outputs["flow_accum_d8"]),
            slope=str(outputs["slope"]),
            output=str(outputs["twi"]),
        )

        # Streams
        outputs["streams"] = output_dir / "streams.tif"
        self.wbt.extract_streams(
            flow_accum=str(outputs["flow_accum_d8"]),
            output=str(outputs["streams"]),
            threshold=1000,
        )

        logger.info("DEM preprocessing pipeline completed")
        logger.info(f"Outputs saved to: {output_dir}")

        with open(output_dir / "preprocessing_metadata.json", "w") as f:
            json.dump(
                {
                    "input_dem": str(input_dem),
                    "fill_method": fill_method,
                    "outputs": {k: str(v) for k, v in outputs.items()},
                },
                f,
                indent=2,
            )

        return outputs
