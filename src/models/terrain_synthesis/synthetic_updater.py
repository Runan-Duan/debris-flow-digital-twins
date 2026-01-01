import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_bounds
from shapely.geometry import shape, Point, Polygon
from scipy.ndimage import gaussian_filter
import geopandas as gpd

logger = logging.getLogger(__name__)


class SyntheticTerrainUpdater:
    """
    Synthetic terrain update module, modify DEM based on simulation results
    or hypothetical scenarios to explore "what-if" situations.
    """
    
    def __init__(self):
        pass
    
    def apply_simulation_changes(
        self,
        base_dem_path: Path,
        simulation_results: Dict[str, Any],
        output_path: Path,
        intensity_factor: float = 1.0
    ) -> Dict[str, Any]:
        """
        Apply terrain changes based on Flow-R simulation results
        
        Args:
            base_dem_path: Path to current DEM
            simulation_results: Results from Flow-R including flow depth/velocity
            output_path: Path for modified DEM
            intensity_factor: Scale factor for changes (0-2, default 1.0)
            
        Returns:
            Dict with modification statistics
        """
        logger.info(f"Applying simulation-based changes to {base_dem_path}")
        
        with rasterio.open(base_dem_path) as src:
            dem = src.read(1)
            profile = src.meta.copy()
            transform = src.transform
            
            # Initialize change raster
            changes = np.zeros_like(dem, dtype=np.float32)
            
            # Process flow depth data from simulation
            if 'flow_depth_raster' in simulation_results:
                flow_depth = self._load_or_create_raster(
                    simulation_results['flow_depth_raster'],
                    dem.shape,
                    transform
                )
                
                # Convert flow depth to deposition
                # Assume material settles where velocity drops
                deposition = self._calculate_deposition(
                    flow_depth,
                    simulation_results.get('velocity_raster'),
                    intensity_factor
                )
                changes += deposition
            
            # Process source areas for erosion
            if 'source_areas' in simulation_results:
                erosion = self._calculate_erosion(
                    simulation_results['source_areas'],
                    dem.shape,
                    transform,
                    intensity_factor
                )
                changes -= erosion
            
            # Apply changes to DEM
            modified_dem = dem + changes
            
            # Ensure no negative elevations
            modified_dem = np.maximum(modified_dem, 0)
            
            # Smooth transitions
            modified_dem = self._smooth_transitions(modified_dem, dem, changes)
            
            # Write output
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(output_path, 'w', **profile) as dst:
                dst.write(modified_dem, 1)
            
            # Calculate statistics
            stats = self._calculate_change_statistics(dem, modified_dem, changes)
            
            logger.info(f"Synthetic terrain created: {output_path}")
            logger.info(f"Total erosion: {stats['total_erosion_m3']:.2f} m³")
            logger.info(f"Total deposition: {stats['total_deposition_m3']:.2f} m³")
            
            return {
                "output_path": str(output_path),
                "statistics": stats,
                "modification_type": "simulation_based"
            }
    
    def apply_hypothetical_erosion(
        self,
        base_dem_path: Path,
        erosion_zones: List[Dict[str, Any]],
        output_path: Path,
        output_version_name: str
    ) -> Dict[str, Any]:
        """
        Apply hypothetical erosion scenarios
        
        Args:
            base_dem_path: Path to current DEM
            erosion_zones: List of erosion zone definitions with geometries and depths
            output_path: Path for modified DEM
            output_version_name: Name for this terrain version
            
        Returns:
            Dict with modification results
            
        Example erosion_zones:
        [
            {
                "geometry": Polygon(...),  # Shapely polygon
                "erosion_depth_m": 2.0,
                "pattern": "uniform" or "channelized",
                "description": "Main channel erosion"
            }
        ]
        """
        logger.info(f"Applying hypothetical erosion to {base_dem_path}")
        
        with rasterio.open(base_dem_path) as src:
            dem = src.read(1)
            profile = src.meta.copy()
            transform = src.transform
            
            # Initialize change raster
            changes = np.zeros_like(dem, dtype=np.float32)
            
            for zone in erosion_zones:
                logger.info(f"Processing zone: {zone.get('description', 'unnamed')}")
                
                # Rasterize polygon
                geom = zone['geometry']
                erosion_mask = rasterize(
                    [(geom, 1)],
                    out_shape=dem.shape,
                    transform=transform,
                    fill=0,
                    dtype=np.uint8
                )
                
                # Apply erosion pattern
                erosion_depth = zone['erosion_depth_m']
                pattern = zone.get('pattern', 'uniform')
                
                if pattern == 'uniform':
                    zone_changes = erosion_mask * (-erosion_depth)
                
                elif pattern == 'channelized':
                    # Create channel-like pattern with deeper center
                    zone_changes = self._create_channel_pattern(
                        erosion_mask,
                        erosion_depth,
                        dem,
                        transform
                    )
                
                elif pattern == 'slope_dependent':
                    # More erosion on steeper slopes
                    slope = self._calculate_slope(dem, transform)
                    zone_changes = erosion_mask * (-erosion_depth) * (slope / 45.0)
                
                else:
                    zone_changes = erosion_mask * (-erosion_depth)
                
                changes += zone_changes
            
            # Apply changes
            modified_dem = dem + changes
            modified_dem = np.maximum(modified_dem, 0)
            
            # Smooth transitions
            modified_dem = self._smooth_transitions(modified_dem, dem, changes)
            
            # Write output
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(output_path, 'w', **profile) as dst:
                dst.write(modified_dem, 1)
            
            stats = self._calculate_change_statistics(dem, modified_dem, changes)
            
            logger.info(f"Hypothetical terrain created: {output_path}")
            
            return {
                "output_path": str(output_path),
                "version_name": output_version_name,
                "statistics": stats,
                "modification_type": "hypothetical",
                "zones": erosion_zones
            }
    
    def apply_deposition_scenario(
        self,
        base_dem_path: Path,
        deposition_zones: List[Dict[str, Any]],
        output_path: Path,
        output_version_name: str
    ) -> Dict[str, Any]:
        """
        Apply hypothetical deposition (e.g., fan building)
        
        Args:
            base_dem_path: Path to current DEM
            deposition_zones: List of deposition zone definitions
            output_path: Path for modified DEM
            output_version_name: Name for this terrain version
            
        Example deposition_zones:
        [
            {
                "geometry": Polygon(...),
                "deposition_depth_m": 1.5,
                "pattern": "fan" or "uniform",
                "apex_point": Point(...),  # For fan pattern
                "description": "Debris fan accumulation"
            }
        ]
        """
        logger.info(f"Applying hypothetical deposition to {base_dem_path}")
        
        with rasterio.open(base_dem_path) as src:
            dem = src.read(1)
            profile = src.meta.copy()
            transform = src.transform
            
            changes = np.zeros_like(dem, dtype=np.float32)
            
            for zone in deposition_zones:
                logger.info(f"Processing zone: {zone.get('description', 'unnamed')}")
                
                geom = zone['geometry']
                deposition_mask = rasterize(
                    [(geom, 1)],
                    out_shape=dem.shape,
                    transform=transform,
                    fill=0,
                    dtype=np.uint8
                )
                
                deposition_depth = zone['deposition_depth_m']
                pattern = zone.get('pattern', 'uniform')
                
                if pattern == 'uniform':
                    zone_changes = deposition_mask * deposition_depth
                
                elif pattern == 'fan':
                    # Create alluvial fan pattern - thicker at apex, thinner at edges
                    apex = zone.get('apex_point')
                    if apex:
                        zone_changes = self._create_fan_pattern(
                            deposition_mask,
                            deposition_depth,
                            apex,
                            dem.shape,
                            transform
                        )
                    else:
                        zone_changes = deposition_mask * deposition_depth
                
                changes += zone_changes
            
            modified_dem = dem + changes
            modified_dem = self._smooth_transitions(modified_dem, dem, changes)
            
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with rasterio.open(output_path, 'w', **profile) as dst:
                dst.write(modified_dem, 1)
            
            stats = self._calculate_change_statistics(dem, modified_dem, changes)
            
            return {
                "output_path": str(output_path),
                "version_name": output_version_name,
                "statistics": stats,
                "modification_type": "deposition_scenario",
                "zones": deposition_zones
            }
    
    def create_progressive_scenario(
        self,
        base_dem_path: Path,
        modification_sequence: List[Dict[str, Any]],
        output_dir: Path,
        base_version_name: str
    ) -> List[Dict[str, Any]]:
        """
        Create a sequence of progressive terrain modifications
        Useful for exploring temporal evolution scenarios
        
        Args:
            base_dem_path: Starting DEM
            modification_sequence: List of modifications to apply sequentially
            output_dir: Directory for output DEMs
            base_version_name: Base name for versions
            
        Returns:
            List of results for each step
        """
        logger.info("Creating progressive scenario sequence")
        
        results = []
        current_dem = base_dem_path
        
        for i, modification in enumerate(modification_sequence):
            version_name = f"{base_version_name}_step{i+1:02d}"
            output_path = output_dir / f"{version_name}.tif"
            
            if modification['type'] == 'erosion':
                result = self.apply_hypothetical_erosion(
                    current_dem,
                    modification['zones'],
                    output_path,
                    version_name
                )
            elif modification['type'] == 'deposition':
                result = self.apply_deposition_scenario(
                    current_dem,
                    modification['zones'],
                    output_path,
                    version_name
                )
            
            result['step'] = i + 1
            result['description'] = modification.get('description', '')
            results.append(result)
            
            current_dem = output_path
        
        logger.info(f"Created {len(results)} progressive terrain versions")
        return results
    
    # ========================================================================
    # Helper Methods
    # ========================================================================
    
    def _load_or_create_raster(
        self,
        raster_path: Path,
        target_shape: Tuple[int, int],
        target_transform
    ) -> np.ndarray:
        """Load and align raster to target grid"""
        
        if not Path(raster_path).exists():
            return np.zeros(target_shape, dtype=np.float32)
        
        with rasterio.open(raster_path) as src:
            # TODO: Add proper resampling if grids don't match
            data = src.read(1)
            if data.shape != target_shape:
                logger.warning("Raster shape mismatch, using zeros")
                return np.zeros(target_shape, dtype=np.float32)
            return data
    
    def _calculate_deposition(
        self,
        flow_depth: np.ndarray,
        velocity_raster: Optional[Path],
        intensity_factor: float
    ) -> np.ndarray:
        """
        Calculate deposition based on flow characteristics
        Simple model: deposition occurs where flow depth > 0 and velocity is low
        """
        deposition = np.zeros_like(flow_depth)
        
        # Areas with flow but low velocity accumulate sediment
        has_flow = flow_depth > 0.1  # meters
        
        if velocity_raster:
            velocity = self._load_or_create_raster(
                velocity_raster,
                flow_depth.shape,
                None
            )
            low_velocity = velocity < 2.0  # m/s
            
            # Deposition proportional to flow depth, inversely to velocity
            deposition[has_flow & low_velocity] = (
                flow_depth[has_flow & low_velocity] * 0.3 * intensity_factor
            )
        else:
            # Without velocity, use simplified approach
            deposition[has_flow] = flow_depth[has_flow] * 0.2 * intensity_factor
        
        # Smooth deposition pattern
        deposition = gaussian_filter(deposition, sigma=2.0)
        
        return deposition
    
    def _calculate_erosion(
        self,
        source_areas: List[Dict],
        shape: Tuple[int, int],
        transform,
        intensity_factor: float
    ) -> np.ndarray:
        """Calculate erosion in source areas"""
        
        erosion = np.zeros(shape, dtype=np.float32)
        
        for source in source_areas:
            if 'geometry' in source:
                mask = rasterize(
                    [(source['geometry'], 1)],
                    out_shape=shape,
                    transform=transform,
                    fill=0,
                    dtype=np.uint8
                )
                
                erosion_depth = source.get('erosion_depth_m', 1.0)
                erosion += mask * erosion_depth * intensity_factor
        
        return erosion
    
    def _create_channel_pattern(
        self,
        mask: np.ndarray,
        max_depth: float,
        dem: np.ndarray,
        transform
    ) -> np.ndarray:
        """Create channelized erosion pattern following flow direction"""
        
        # Calculate flow direction
        from scipy.ndimage import sobel
        
        grad_x = sobel(dem, axis=1)
        grad_y = sobel(dem, axis=0)
        
        # Distance from channel center (simplified)
        from scipy.ndimage import distance_transform_edt
        
        channel_center = mask.astype(bool)
        distances = distance_transform_edt(~channel_center)
        
        # Gaussian profile - deeper in center, shallow at edges
        max_dist = distances[mask > 0].max() if distances[mask > 0].size > 0 else 1
        if max_dist > 0:
            profile = np.exp(-0.5 * (distances / (max_dist / 3))**2)
        else:
            profile = mask
        
        changes = -mask * profile * max_depth
        
        return changes
    
    def _create_fan_pattern(
        self,
        mask: np.ndarray,
        max_depth: float,
        apex: Point,
        shape: Tuple[int, int],
        transform
    ) -> np.ndarray:
        """Create alluvial fan deposition pattern"""
        
        from scipy.ndimage import distance_transform_edt
        
        # Convert apex to pixel coordinates
        col, row = ~transform * (apex.x, apex.y)
        col, row = int(col), int(row)
        
        # Calculate distance from apex
        y_coords, x_coords = np.ogrid[0:shape[0], 0:shape[1]]
        distances = np.sqrt((x_coords - col)**2 + (y_coords - row)**2)
        
        # Normalize within mask
        max_dist = distances[mask > 0].max() if distances[mask > 0].size > 0 else 1
        if max_dist > 0:
            normalized_dist = distances / max_dist
        else:
            normalized_dist = distances
        
        # Exponential decay from apex
        decay = np.exp(-2 * normalized_dist)
        
        deposition = mask * decay * max_depth
        
        return deposition
    
    def _calculate_slope(
        self,
        dem: np.ndarray,
        transform
    ) -> np.ndarray:
        """Calculate slope in degrees"""
        
        from scipy.ndimage import sobel
        
        # Get resolution
        res_x = abs(transform[0])
        res_y = abs(transform[4])
        
        # Calculate gradients
        grad_x = sobel(dem, axis=1) / (8 * res_x)
        grad_y = sobel(dem, axis=0) / (8 * res_y)
        
        # Calculate slope
        slope = np.sqrt(grad_x**2 + grad_y**2)
        slope_degrees = np.degrees(np.arctan(slope))
        
        return slope_degrees
    
    def _smooth_transitions(
        self,
        modified_dem: np.ndarray,
        original_dem: np.ndarray,
        changes: np.ndarray,
        sigma: float = 1.5
    ) -> np.ndarray:
        """Smooth transitions between modified and unmodified areas"""
        
        # Identify transition zones
        change_magnitude = np.abs(changes)
        has_changes = change_magnitude > 0.01
        
        # Create transition mask
        from scipy.ndimage import binary_dilation
        transition_zone = binary_dilation(has_changes, iterations=5) & ~has_changes
        
        # Smooth only in transition zones
        smoothed = gaussian_filter(modified_dem, sigma=sigma)
        
        result = modified_dem.copy()
        result[transition_zone] = (
            0.7 * modified_dem[transition_zone] + 
            0.3 * smoothed[transition_zone]
        )
        
        return result
    
    def _calculate_change_statistics(
        self,
        original: np.ndarray,
        modified: np.ndarray,
        changes: np.ndarray
    ) -> Dict[str, float]:
        """Calculate statistics about terrain changes"""
        
        # Assume 1m resolution for volume calculations
        # TODO: Get actual resolution from transform
        pixel_area = 1.0  # m²
        
        erosion = changes < -0.01
        deposition = changes > 0.01
        
        stats = {
            "total_erosion_m3": float(np.sum(np.abs(changes[erosion])) * pixel_area),
            "total_deposition_m3": float(np.sum(changes[deposition]) * pixel_area),
            "net_change_m3": float(np.sum(changes) * pixel_area),
            "max_erosion_m": float(np.abs(changes[erosion].min())) if erosion.any() else 0.0,
            "max_deposition_m": float(changes[deposition].max()) if deposition.any() else 0.0,
            "affected_area_m2": float(np.sum(np.abs(changes) > 0.01) * pixel_area),
            "mean_elevation_change_m": float(np.mean(changes)),
            "std_elevation_change_m": float(np.std(changes))
        }
        
        return stats