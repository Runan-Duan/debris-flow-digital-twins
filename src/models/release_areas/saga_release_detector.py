import logging
from pathlib import Path
from typing import Dict, Any, Optional
from PySAGA_cmd import SAGA
import numpy as np
import geopandas as gpd
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape, box
from shapely.ops import unary_union

logger = logging.getLogger(__name__)


class SAGAReleaseDetector:
    """
    Identify debris flow release areas using SAGA GIS
    """
    
    def __init__(self, saga_cmd_path: str = r"D:\Applications\saga\saga-9.10.2_x64\saga_cmd.exe"):
        """
        Initialize SAGA release detector
        
        Args:
            saga_cmd_path: Path to saga_cmd.exe (use raw string r"..." for Windows paths)
        """
        self.saga = SAGA(saga_cmd_path)
        self.grid_calculus = self.saga / 'grid_calculus'
        self.grid_filter = self.saga / 'grid_filter'
        self.shapes_grid = self.saga / 'shapes_grid'
        self.grid_analysis = self.saga / 'grid_analysis'
        self.shapes_polygons = self.saga / 'shapes_polygons'
        
        logger.info(f"SAGA initialized: {saga_cmd_path}")
    
    def identify_release_areas(
        self,
        preprocessed_data: Dict[str, Path],
        dsm_path: Optional[Path] = None,
        dtm_path: Optional[Path] = None,
        output_dir: Path = None
    ) -> Path:
        """
        Identify release areas using SAGA GIS grid calculator
        
        Multi-criteria approach:
        - Slope: 15-45 degrees
        - Plan curvature < -0.01 (convergent/gullies)
        - Flow accumulation > 500 cells
        - Vegetation height < 2m (if DSM/DTM available)
        - TRI > threshold (rough terrain)
        
        Returns:
            Path to release areas shapefile
        """
        logger.info("SAGA GIS Release Area Identification")
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create slope mask (15-50 degrees)
        logger.info("\n1. Creating slope criterion (15-50 degrees) ...")
        slope_mask = output_dir / "slope_mask.sdat"
        
        formula_slope = "ifelse( and(gt(atan(g1) * 180 / pi(), 15), lt(atan(g1) * 180 / pi(), 50)), 1, 0)"

        calc_tool = self.grid_calculus / 'Grid Calculator'
        
        try:
            calc_tool.execute(
                verbose=True,
                grids=str(preprocessed_data['slope']),
                result=str(slope_mask),
                formula=formula_slope,
                name='Slope_Mask',
                use_nodata=True,
                type=3  # Integer output
            )
            logger.info(" Slope mask created")
        except Exception as e:
            logger.error(f" Slope mask failed: {e}")
            raise
        
        # Plan curvature mask (< -0.01)
        logger.info("\n2. Creating plan curvature criterion (< -0.01)...")
        curv_mask = output_dir / "curvature_mask.sdat"
        
        formula_curv = "ifelse(lt(g1,-0.01), 1, 0)"
        
        try:
            calc_tool.execute(
                verbose=True,
                grids=str(preprocessed_data['plan_curvature']),
                result=str(curv_mask),
                formula=formula_curv,
                name='Curvature_Mask',
                use_nodata=True,
                type=3
            )
            logger.info(" Curvature mask created")
        except Exception as e:
            logger.error(f" Curvature mask failed: {e}")
            raise
        
        # Flow accumulation mask (> 500)
        logger.info("\n3. Creating flow accumulation criterion (> 500 cells)...")
        flow_mask = output_dir / "flow_mask.sdat"
        
        formula_flow = "ifelse(gt(g1,500), 1, 0)"
        
        try:
            calc_tool.execute(
                verbose=True,
                grids=str(preprocessed_data['flow_accum_d8']),
                result=str(flow_mask),
                formula=formula_flow,
                name='Flow_Mask',
                use_nodata=True,
                type=3
            )
            logger.info(" Flow accumulation mask created")
        except Exception as e:
            logger.error(f" Flow accumulation mask failed: {e}")
            raise
        
        # TRI mask (rough terrain)
        logger.info("\n4. Creating roughness criterion...")
        
        # First normalize TRI to 0-1
        tri_norm = output_dir / "tri_normalized.sdat"
        standardize_tool = self.grid_calculus / 'Grid Standardization'

        try:
            standardize_tool.execute(
                verbose=True,
                input=str(preprocessed_data['tri']),
                output=str(tri_norm),
                stretch=1  # 0-1 range
            )
            logger.info(" TRI normalized")
        except Exception as e:
            logger.error(f" TRI normalization failed: {e}")
            raise
        
        # Then threshold
        rough_mask = output_dir / "roughness_mask.sdat"
        formula_rough = "ifelse(gt(g1,0.3), 1, 0)"
        
        try:
            calc_tool.execute(
                verbose=True,
                grids=str(tri_norm),
                result=str(rough_mask),
                formula=formula_rough,
                name='Roughness_Mask',
                use_nodata=True,
                type=3
            )
            logger.info(" Roughness mask created")
        except Exception as e:
            logger.error(f" Roughness mask failed: {e}")
            raise
        
        # Vegetation mask 
        logger.info("\n5. Creating vegetation criterion (DSM - DTM)...")
        
        # Calculate vegetation height
        veg_height = output_dir / "veg_height.sdat"
        # When using multiple grids: g1 = first grid, g2 = second grid
        formula_veg_height = "g1 - g2"
        
        try:
            calc_tool.execute(
                verbose=True,
                grids=f"{dsm_path};{dtm_path}",  # Semicolon-separated
                result=str(veg_height),
                formula=formula_veg_height,
                name='Vegetation_Height',
                use_nodata=True,
                type=9  # Float output
            )
            logger.info(" Vegetation height calculated")
        except Exception as e:
            logger.error(f" Vegetation height failed: {e}")
            raise
        
        # Threshold: bare areas (< 2m vegetation)
        bare_mask = output_dir / "bare_mask.sdat"
        formula_bare = "ifelse(lt(g1,2.0), 1, 0)"
        
        try:
            calc_tool.execute(
                verbose=True,
                grids=str(veg_height),
                result=str(bare_mask),
                formula=formula_bare,
                name='Bare_Mask',
                use_nodata=True,
                type=3
            )
            logger.info(" Bare areas mask created")
        except Exception as e:
            logger.error(f" Bare areas mask failed: {e}")
            raise
        
        # Combine all criteria (INCLUDING vegetation)
        logger.info("\n6. Combining all criteria (including vegetation)...")
        release_raster = output_dir / "release_areas.sdat"
        
        # g1*g2*g3*g4*g5 = 1 only where ALL are 1
        formula_combined = "g1 * g2 * g3 * g4"
        
        try:
            calc_tool.execute(
                verbose=True,
                grids=f"{slope_mask};{curv_mask};{rough_mask};{bare_mask}",
                result=str(release_raster),
                formula=formula_combined,
                name='Release_Areas',
                use_nodata=True,
                type=3
            )
            logger.info(" All criteria combined (with vegetation)")
        except Exception as e:
            logger.error(f" Combining criteria failed: {e}")
            raise
        
        # Convert raster to polygons
        # logger.info("\n7. Converting raster to polygons...")
        # raw_polygons = output_dir / "release_raw.shp"
        # release_polygons = output_dir / "release_final.shp"
        # buffer_distance = 10.0

        # Read raster with rasterio
        # try:
        #     with rasterio.open(release_raster) as src:
        #         data = src.read(1)
        #         transform = src.transform
        #         crs = src.crs
                
        #         logger.info(f"  Raster: {data.shape}, Non-zero: {np.sum(data > 0)} pixels")
                
        #         # Create mask
        #         mask = (data == 1).astype('uint8')
                
        #         # Vectorize with Rasterio (FAST!)
        #         logger.info("  Vectorizing with Rasterio...")
        #         geoms, values= [], []
        #         for geom, value in shapes(mask, mask=(mask == 1), transform=transform):
        #             if value == 1:
        #                 geoms.append(shape(geom))
        #                 values.append(value)

        #         logger.info(f" Extracted {len(geoms)} polygons")
                
        #         # Create GeoDataFrame
        #         gdf = gpd.GeoDataFrame({'value': values}, geometry=geoms, crs=crs)
                
        #         # Filter small polygons
        #         gdf['area'] = gdf.geometry.area
        #         # gdf = gdf[gdf['area'] >= 100]  # Minimum 100 m²
        #         # logger.info(f" Filtered to {len(gdf)} polygons (>100m²)")
                
        #         # Save raw
        #         gdf.to_file(raw_polygons)
                
        # except Exception as e:
        #     logger.error(f" Rasterio vectorization failed: {e}")
        #     raise     

        # vectorize_tool = self.shapes_grid / 6 # Tool number 6 = "Vectorising Grid Classes"

        # try:
        #     vectorize_tool.execute(
        #         verbose=True,
        #         grid=str(release_raster),
        #         polygons=str(raw_polygons),
        #         class_all=1,
        #         class_id=1.0,
        #         split=1,
        #         ALLVERTICES=0
        #     )
        #     logger.info(" Vectorization complete")
        # except Exception as e:
        #     logger.error(f" Vectorization failed: {e}")
        #     raise

        # try:
        #     logger.info("Loading polygons")
        #     gdf = gpd.read_file(raw_polygons)
        #     dissolved = gdf.dissolve()
        #     buffered = dissolved.buffer(buffer_distance)
        #     if len(buffered) > 1:
        #         from shapely.ops import unary_union
        #         buffered = gpd.GeoSeries([unary_union(buffered)], crs=gdf.crs)
        #     smoothed = buffered.buffer(-buffer_distance)
        #     result_gdf = gpd.GeoDataFrame({'geometry': smoothed}, crs=gdf.crs)
        #     result_gdf.to_file(release_polygons)

        #     logger.info("Polygons dissolved")
        # except Exception as e:
        #     logger.error(f"Failed to dissolve polygons")

        # logger.info("RELEASE AREA IDENTIFICATION COMPLETE")
        # logger.info(f"Output shapefile: {release_polygons}")
        
        return release_raster
    