import logging
from pathlib import Path
from typing import Dict, Any
from PySAGA_cmd import SAGA
import geopandas as gpd

logger = logging.getLogger(__name__)


class SAGAGPPRunner:
    """
    Run SAGA GIS Gravitational Process Path model
    """
    
    def __init__(self, saga_cmd_path: str = r"D:\Applications\saga\saga-9.10.2_x64\saga_cmd.exe"):
        self.saga = SAGA(saga_cmd_path)
        self.gpp_lib = self.saga / 'sim_geomorphology'
        self.grid_gridding = self.saga / 'grid_gridding'
        self.grid_tools = self.saga / 'grid_tools'
    
    def run_gpp_simulation(
        self,
        dem_path: Path,
        release_areas_shp: Path,
        output_dir: Path,
        friction_model: int = 4,  
        friction_angle: float = 32.0,
        friction_mu: float = 0.25,  
        mass_to_drag: float = 200.0,  
        iterations: int = 10,  
    ) -> Dict[str, Any]:
        """
        Run SAGA GPP simulation
        
        Args:
            dem_path: Filled DEM (SAGA grid format .sdat or GeoTIFF)
            release_areas_shp: Shapefile of release areas (polygons)
            output_dir: Output directory
            friction_model: Friction model selection
                0 = None
                1 = Geometric Gradient (Heim 1932)
                2 = Fahrboeschung Principle (Heim 1932)
                3 = Shadow Angle (Evans & Hungr 1988)
                4 = 1-parameter friction model (Scheidegger 1975) 
                5 = PCM Model (Perla et al. 1980) 
            friction_angle: Friction angle in degrees (for models 1-3)
            friction_mu: Friction parameter mu (for models 4-5)
            mass_to_drag: Mass-to-drag ratio in meters (for PCM model)
            iterations: Number of GPP iterations (default 1000)
            
        Returns:
            Dictionary with results
        """
        logger.info("SAGA GIS GPP DEBRIS FLOW SIMULATION")
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Convert shapefile to raster with unique IDs
        logger.info("\nStep 1: Converting release areas to raster...")
        release_raster = output_dir / "release_polygon_grid.sdat"
        
        shapes_to_grid = self.grid_gridding / '0'
        
        shapes_to_grid.execute(
            verbose=True,
            INPUT=str(release_areas_shp),
            GRID=str(release_raster),
            TARGET_TEMPLATE=str(dem_path),
            FIELD=0,                      # Attribute field to use (0 = first field)
            OUTPUT=1,                     # 1 = index number (creates unique IDs for each polygon)
            MULTIPLE=1,                   # Method for multiple values: 1 = last
            POLY_TYPE=1,                  # Polygon rasterization: 1 = cell
            GRID_TYPE=2,                  # Grid type: 2 = integer (required for release areas)
            TARGET_DEFINITION=1,          # 1 = grid or grid system (use template)
        )
        logger.info("Release areas converted to raster with unique IDs")
        
        # Resampling 
        resampling = self.grid_tools / "Resampling"
        release_grid = output_dir / 'release_polygon_resample.sdat'
        resampling.execute(
            INPUT=str(release_raster),
            TARGET_TEMPLATE=str(dem_path),
            OUTPUT=str(release_grid),        # Changed from RESULT to OUTPUT
            KEEP_TYPE=True,                  # Preserve integer data type
            SCALE_UP=0,                      # Nearest Neighbour for upscaling
            SCALE_DOWN=0,                    # Nearest Neighbour for downscaling
            TARGET_DEFINITION=1,             # 1 = use grid or grid system (template)
        )
        logger.info("Resampled release grid")

        # Run GPP Model
        logger.info("\nStep 2: Running GPP model...")
        logger.info(f"  Process path model: Random Walk")
        logger.info(f"  Friction model: {friction_model} (4=1-param, 5=PCM)")
        logger.info(f"  Friction mu: {friction_mu}")
        if friction_model == 5:
            logger.info(f"  Mass-to-drag ratio: {mass_to_drag} m")
        
        logger.info(f"  Iterations: {iterations}")
        
        gpp_tool = self.gpp_lib / 0  # Tool 0 is the GPP model
        
        process_area = output_dir / "process_area.sdat"
        deposition = output_dir / "deposition.sdat"
        max_velocity = output_dir / "max_velocity.sdat"
        stop_positions = output_dir / "stop_positions.sdat"
        
        try:
            # Build parameters dynamically based on friction model
            params = {
                'verbose': True,
                'DEM': str(dem_path),
                'RELEASE_AREAS': str(release_raster),
                'PROCESS_AREA': str(process_area),
                'DEPOSITION': str(deposition),
                'PROCESS_PATH_MODEL': 1,           # 1 = Random Walk
                'RW_SLOPE_THRES': 40.0,            # Slope threshold for lateral spreading
                'RW_EXPONENT': 3.0,                # Exponent for lateral spreading (2.0-3.0 typical)
                'RW_PERSISTENCE': 1.5,             # Persistence factor (reduces abrupt direction changes)
                'GPP_ITERATIONS': iterations,
                'GPP_PROCESSING_ORDER': 2,         # 2 = RAs in parallel per iteration (recommended)
                'GPP_SEED': 1,                     # Random seed (1 = use current time)
                'FRICTION_MODEL': friction_model,
                'FRICTION_THRES_FREE_FALL': 60.0,  # Minimum slope for free fall
                'SINK_MIN_SLOPE': 2.5              # Minimum slope to preserve on sink filling
            }
            
            # Add friction parameters based on model
            if friction_model == 4:
                params['FRICTION_MU'] = friction_mu
                params['FRICTION_METHOD_IMPACT'] = 0      # 0 = Energy Reduction
                params['FRICTION_IMPACT_REDUCTION'] = 75.0  # Energy reduction on impact (%)
                params['FRICTION_MODE_OF_MOTION'] = 0     # 0 = Sliding
                params['MAX_VELOCITY'] = str(max_velocity)
            
            if friction_model == 5:
                params['FRICTION_MU'] = friction_mu
                params['FRICTION_MASS_TO_DRAG'] = mass_to_drag
                params['FRICTION_INIT_VELOCITY'] = 1.0
                params['MAX_VELOCITY'] = str(max_velocity)
            
            # Add optional outputs
            params['STOP_POSITIONS'] = str(stop_positions)
            gpp_tool.execute(**params)
            logger.info("GPP simulation completed successfully")
            
        except Exception as e:
            logger.error(f"GPP simulation failed: {e}")
            logger.error("Check that:")
            logger.error("  1. DEM is in SAGA grid format (.sdat/.sgrd)")
            logger.error("  2. Release areas shapefile contains valid polygons")
            logger.error("  3. Both files are in the same coordinate system")
            raise
        
        logger.info("SIMULATION COMPLETE")
        
        results = {
            "process_area": str(process_area),
            "stop_positions": str(stop_positions),
            "parameters": {
                "friction_model": friction_model,
                "friction_angle": friction_angle if friction_model in [1,2,3] else None,
                "friction_mu": friction_mu if friction_model in [4,5] else None,
                "mass_to_drag": mass_to_drag if friction_model == 5 else None,
                "iterations": iterations,
                "process_path_model": "Random Walk",
                "rw_slope_threshold": 40.0,
                "rw_exponent": 3.0,
                "rw_persistence": 1.5
            }
        }

        results["max_velocity"] = str(max_velocity)
        
        logger.info(f"\nOutputs saved to: {output_dir}")
        logger.info(f"  - Process area: {process_area.name}")
        logger.info(f"  - Stop positions: {stop_positions.name}")
        logger.info(f"  - Max velocity: {max_velocity.name}")
        
        return results