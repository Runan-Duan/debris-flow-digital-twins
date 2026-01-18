import logging
from pathlib import Path
from typing import Dict, Any, Optional
from PySAGA_cmd import SAGA, get_sample_dem, Tool
import json

logger = logging.getLogger(__name__)


class SAGAPreprocessor:
    """
    All preprocessing using SAGA GIS tools
    - Sink removal / filling
    - Flow accumulation
    - Morphometry (slope, curvature, etc.)
    """
    
    def __init__(self, saga_cmd_path: str = "D:\Applications\saga\saga-9.10.2_x64\saga_cmd.exe"):
        """
        Initialize SAGA preprocessor
        
        Args:
            saga_cmd_path: Path to saga_cmd executable
        """
        self.saga = SAGA(saga_cmd_path)
        logger.info(f"SAGA GIS initialized: {saga_cmd_path}")
    
    def preprocess_complete_pipeline(
        self,
        input_dem: Path,
        output_dir: Path,
        method: str = "fill"  # "fill" or "breach"
    ) -> Dict[str, Path]:
        """
        Complete preprocessing pipeline using ONLY SAGA
        
        Args:
            input_dem: Path to input DEM (any format SAGA supports: TIF, ASC, SDAT, etc.)
            output_dir: Directory for outputs
            method: "fill" (Fill Sinks) or "breach" (Deepen Drainage Routes)
            
        Returns:
            Dictionary of output file paths
        """
        logger.info(f"SAGA GIS Preprocessing Pipeline")
        logger.info(f"Input DEM: {input_dem}")
        logger.info(f"Method: {method}")
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        outputs = {}
        
        # Access SAGA libraries
        preprocessor = self.saga / 'ta_preprocessor'
        morphometry = self.saga / 'ta_morphometry'
        hydrology = self.saga / 'ta_hydrology'
        channels = self.saga / 'ta_channels'
        
        # Sink Drainage Route Detection
        logger.info("\n1. Detecting sink drainage routes...")
        route_detection = preprocessor / 'Sink Drainage Route Detection'
        
        sinkroute_path = output_dir / "sinkroute.sdat"
        
        route_out = route_detection.execute(
            verbose=True,
            elevation=str(input_dem),
            sinkroute=str(sinkroute_path),
            threshold=True,
            thrsheight=100.0
        )
        
        # Sink Removal (Fill or Deepen)
        logger.info(f"\n2. Removing sinks using method: {method}...")
        sink_removal = preprocessor / 'Sink Removal'
        
        outputs['filled_dem'] = output_dir / "dem_filled.sdat"
        
        sink_out = sink_removal.execute(
            verbose=True,
            dem=str(input_dem),
            sinkroute=str(sinkroute_path),
            dem_preproc=str(outputs['filled_dem']),
            method='Fill Sinks' if method == 'fill' else 'Deepen Drainage Routes'
        )
        
        logger.info(" DEM preprocessing completed")
        
        # Slope, Aspect, Curvature
        logger.info("\n3. Calculating morphometry...")
        slope_aspect_curvature = morphometry / 'Slope, Aspect, Curvature'
        
        outputs['slope'] = output_dir / "slope.sdat"
        outputs['aspect'] = output_dir / "aspect.sdat"
        outputs['plan_curvature'] = output_dir / "plan_curv.sdat"
        outputs['profile_curvature'] = output_dir / "profile_curv.sdat"
        outputs['tangential_curvature'] = output_dir / "tang_curv.sdat"
        
        morph_out = slope_aspect_curvature.execute(
            verbose=True,
            elevation=str(outputs['filled_dem']),
            slope=str(outputs['slope']),
            aspect=str(outputs['aspect']),
            c_plan=str(outputs['plan_curvature']),
            c_prof=str(outputs['profile_curvature']),
            c_tang=str(outputs['tangential_curvature']),
            unit_slope='degrees',
            unit_aspect='degrees'
        )
        
        logger.info(" Morphometry calculated")
        
        # Topographic Ruggedness Index
        logger.info("\n4. Calculating terrain ruggedness...")
        tri_tool = morphometry / 'Terrain Ruggedness Index (TRI)'
        
        outputs['tri'] = output_dir / "tri.sdat"
        
        tri_out = tri_tool.execute(
            verbose=True,
            dem=str(outputs['filled_dem']),
            tri=str(outputs['tri'])
        )
        
        logger.info(" TRI calculated")
        
        # Flow Accumulation (Multiple Algorithms)
        logger.info("\n5. Calculating flow accumulation...")
        
        # D8 Flow Accumulation
        flow_acc_d8 = hydrology / 'Flow Accumulation (Top-Down)'
        outputs['flow_accum_d8'] = output_dir / "flow_accum_d8.sdat"
        
        flow_out = flow_acc_d8.execute(
            verbose=True,
            elevation=str(outputs['filled_dem']),
            flow=str(outputs['flow_accum_d8']),
            method='Deterministic 8'
        )
        
        # Multiple Flow Direction (better for natural terrain!)
        flow_acc_mfd = hydrology / 'Flow Accumulation (Top-Down)'
        outputs['flow_accum_mfd'] = output_dir / "flow_accum_mfd.sdat"
        
        flow_mfd_out = flow_acc_mfd.execute(
            verbose=True,
            elevation=str(outputs['filled_dem']),
            flow=str(outputs['flow_accum_mfd']),
            method='Multiple Flow Direction'
        )
        
        logger.info(" Flow accumulation calculated (D8 + MFD)")
        
        # Topographic Wetness Index
        logger.info("\n6. Calculating topographic wetness index...")

        twi_tool = hydrology / '20'   # Topographic Wetness Index
        outputs['twi'] = output_dir / "twi.sdat"

        twi_out = twi_tool.execute(
            verbose=True,
            slope=str(outputs['slope']),
            area=str(outputs['flow_accum_mfd']),
            twi=str(outputs['twi']),
            conv=1,        # convert accumulation -> specific catchment area
            method=0       # standard TWI
        )
        
        # Stream Network Extraction
        logger.info("\n7. Extracting stream network...")
        stream_tool = channels / "0"    # Channel Network

        for threshold in [3000, 6000, 12000]:
            logger.info(f"Initial value {threshold}")
            out = output_dir / f"streams_{threshold}.sdat"

            stream_tool.execute(
                verbose=True,
                ELEVATION=str(outputs['filled_dem']),
                INIT_GRID=str(outputs['flow_accum_d8']),
                INIT_VALUE=threshold,
                CHNLNTWRK=str(out),
                CHNLROUTE='temp.sdat',
                SHAPES=str(output_dir / f"streams_{threshold}.shp")
            )

        logger.info(" Stream network extracted")
        
        # Save metadata
        metadata = {
            "input_dem": str(input_dem),
            "method": method,
            "outputs": {k: str(v) for k, v in outputs.items()}
        }
        
        with open(output_dir / "preprocessing_metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(" COMPLETE PREPROCESSING PIPELINE FINISHED")
        logger.info(f"All outputs saved to: {output_dir}")
        
        return outputs