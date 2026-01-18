
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from shapely.geometry import box, mapping
import numpy as np
from config.settings import settings

logger = logging.getLogger(__name__)


class TerrainIngestor:
    """
    Terrain data ingestion module for importing DEM, DTM, and orthophoto data
    """
    
    def __init__(self, target_epsg: Optional[int] = None):
        """
        Initialize terrain ingestor
        
        Args:
            target_epsg: Target coordinate system (default from settings)
        """
        self.target_epsg = target_epsg or settings.DEM_EPSG
        self.raw_dir = settings.RAW_DATA_DIR
        self.processed_dir = settings.PROCESSED_DATA_DIR
        
    def ingest_baseline(
        self,
        dem_path: Path,
        version_name: str,
        dtm_path: Optional[Path] = None,
        ortho_path: Optional[Path] = None,
        source: str = "baseline",
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Ingest baseline terrain data
        
        Args:
            dem_path: Path to DEM file
            version_name: Unique version identifier
            dtm_path: DTM path
            ortho_path: orthophoto path
            source: Data source identifier
            metadata: Additional metadata
            
        Returns:
            Dict with ingestion results including paths and metadata
        """
        logger.info(f"Starting baseline ingestion: {version_name}")
        
        try:
            # Validate and process DEM, DTM & ortho
            dem_info = self._process_raster(
                dem_path, 
                f"{version_name}_dem",
            )
            
            dtm_info = self._process_raster(
                dtm_path,
                f"{version_name}_dtm",
            )
        
            ortho_info = self._process_raster(
                ortho_path,
                f"{version_name}_ortho",
            )
            
            # Prepare result
            result = {
                "version_name": version_name,
                "timestamp": datetime.utcnow(),
                "dem_path": str(dem_info["output_path"]),
                "dtm_path": str(dtm_info["output_path"]),
                "ortho_path": str(ortho_info["output_path"]),
                "resolution_m": dem_info["resolution"],
                "epsg_code": self.target_epsg,
                "extent_wgs84": dem_info["extent_wgs84"],
                "extent_native": dem_info["extent_native"],
                "source": source,
                "statistics": {
                    "min_elevation": float(dem_info["stats"]["min"]),
                    "max_elevation": float(dem_info["stats"]["max"]),
                    "mean_elevation": float(dem_info["stats"]["mean"]),
                    "std_elevation": float(dem_info["stats"]["std"]),
                },
                "metadata": metadata or {}
            }
            
            logger.info(f"Baseline ingestion completed: {version_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error during baseline ingestion: {str(e)}")
            raise
    
    def _process_raster(
        self,
        input_path: Path,
        output_name: str,
    ) -> Dict[str, Any]:
        """
        Process and standardize raster data
        
        Args:
            input_path: Path to input raster
            output_name: Name for output file
            is_dem: Whether this is elevation data
            
        Returns:
            Dict with processing results
        """
        logger.debug(f"Processing raster: {input_path}")
        
        with rasterio.open(input_path) as src:
            # Check if reprojection is needed
            if src.crs.to_epsg() != self.target_epsg:
                logger.info(f"Reprojecting from {src.crs} to EPSG:{self.target_epsg}")
                return self._reproject_raster(src, output_name)
            else:
                logger.info("Raster already in target CRS, copying...")
                return self._copy_raster(src, output_name)
    
    def _reproject_raster(
        self,
        src: rasterio.DatasetReader,
        output_name: str,
    ) -> Dict[str, Any]:
        """Reproject raster to target CRS"""
        
        # Calculate transformation
        dst_crs = f"EPSG:{self.target_epsg}"
        transform, width, height = calculate_default_transform(
            src.crs,
            dst_crs,
            src.width,
            src.height,
            *src.bounds
        )
        
        # Output path
        output_path = self.processed_dir / "terrain_snapshots" / f"{output_name}.tif"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare output metadata
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height,
            'compress': 'lzw',
            'tiled': True,
            'blockxsize': 256,
            'blockysize': 256
        })
        
        # Reproject
        with rasterio.open(output_path, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear
                )
        
        # Get statistics and metadata
        return self._extract_raster_info(output_path)
    
    def _copy_raster(
        self,
        src: rasterio.DatasetReader,
        output_name: str,
    ) -> Dict[str, Any]:
        """Copy raster with optimization"""
        
        output_path = self.processed_dir / "terrain_snapshots" / f"{output_name}.tif"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Update metadata for optimization
        kwargs = src.meta.copy()
        kwargs.update({
            'compress': 'lzw',
            'tiled': True,
            'blockxsize': 256,
            'blockysize': 256
        })
        
        # Copy with new metadata
        with rasterio.open(output_path, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                dst.write(src.read(i), i)
        
        return self._extract_raster_info(output_path)
    
    def _extract_raster_info(
        self,
        raster_path: Path,
    ) -> Dict[str, Any]:
        """Extract metadata and statistics from raster"""
        
        with rasterio.open(raster_path) as src:
            # Read first band for statistics
            data = src.read(1, masked=True)
            
            # Calculate statistics
            stats = {
                "min": np.nanmin(data),
                "max": np.nanmax(data),
                "mean": np.nanmean(data),
                "std": np.nanstd(data)
            }
            
            # Get extents
            bounds = src.bounds
            extent_native = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            
            # Convert to WGS84 for storage
            from pyproj import Transformer
            transformer = Transformer.from_crs(
                src.crs,
                "EPSG:4326",
                always_xy=True
            )
            
            # Transform corners
            left, bottom = transformer.transform(bounds.left, bounds.bottom)
            right, top = transformer.transform(bounds.right, bounds.top)
            extent_wgs84 = box(left, bottom, right, top)
            
            # Resolution
            resolution = abs(src.transform[0])
            
            return {
                "output_path": raster_path,
                "resolution": resolution,
                "width": src.width,
                "height": src.height,
                "extent_native": extent_native.wkt,
                "extent_wgs84": extent_wgs84.wkt,
                "stats": stats,
                "crs": str(src.crs),
                "nodata": src.nodata
            }
    
    def validate_dem(self, dem_path: Path) -> Tuple[bool, str]:
        """
        Validate DEM file
        
        Args:
            dem_path: Path to DEM
            
        Returns:
            Tuple of (is_valid, message)
        """
        try:
            with rasterio.open(dem_path) as src:
                # Check if file can be opened
                if src.count < 1:
                    return False, "DEM has no bands"
                
                # Check data type
                if not np.issubdtype(src.dtypes[0], np.floating) and \
                   not np.issubdtype(src.dtypes[0], np.integer):
                    return False, f"Invalid data type: {src.dtypes[0]}"
                
                # Check for CRS
                if src.crs is None:
                    return False, "DEM has no coordinate reference system"
                
                # Read sample to check for data
                sample = src.read(1, window=((0, min(100, src.height)), 
                                            (0, min(100, src.width))))
                
                if np.all(np.isnan(sample)):
                    return False, "DEM contains only NoData values"
                
                return True, "DEM validation passed"
                
        except Exception as e:
            return False, f"Error reading DEM: {str(e)}"
