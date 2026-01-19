import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.models.risk.risk_calculator import RiskCalculator
from src.models.release_areas.saga_release_detector import SAGAReleaseDetector
from src.models.rainfall.rainfall_threshold import SoilSaturationModel
import numpy as np


def compute_rainfall_adjusted_release_areas():
    """
    Compute release areas with rainfall-adjusted slope thresholds
    Integrates risk assessment with SAGA terrain analysis
    """
    
    print("Rainfall-Based Release Area Detection")
    
    risk_calc = RiskCalculator(
        alpha=14.0,
        beta=0.4,
        field_capacity_mm=100.0,
        base_critical_slope_deg=35.0
    )
    
    assessment = risk_calc.calculate_current_risk()
    
    print(f"\nRisk Level: {assessment['risk_level']}")
    print(f"Saturation: {assessment['saturation']:.2f}")
    print(f"Critical slope: {assessment['critical_slope_deg']:.1f} degrees")
    
    if not risk_calc.should_trigger_simulation(assessment):
        print("\nRisk level too low for simulation")
        return
    
    print("\nInitializing SAGA release area detector...")
    
    saga_detector = SAGAReleaseDetector(
        saga_cmd_path=r"D:\Applications\saga\saga-9.10.2_x64\saga_cmd.exe"
    )
    
    preprocessed_data = {
        'slope': Path('data/processed/preprocessing/slope.sdat'),
        'plan_curvature': Path('data/processed/preprocessing/plan_curvature.sdat'),
        'flow_accum_d8': Path('data/processed/preprocessing/flow_accum.sdat'),
        'tri': Path('data/processed/preprocessing/tri.sdat')
    }
    
    dsm_path = Path('data/raw/dem/dsm.tif')
    dtm_path = Path('data/raw/dtm/dtm.tif')
    output_dir = Path('data/processed/release_areas/rainfall_based')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("\nComputing rainfall-adjusted slope mask...")
    
    saturation = assessment['saturation']
    critical_slope = assessment['critical_slope_deg']
    
    min_slope = max(15, critical_slope - 10)
    max_slope = critical_slope + 15
    
    print(f"  Adjusted slope range: {min_slope:.1f} - {max_slope:.1f} degrees")
    
    slope_mask = output_dir / "rainfall_slope_mask.sdat"
    formula_slope = f"ifelse(and(gt(atan(g1)*180/pi(),{min_slope}),lt(atan(g1)*180/pi(),{max_slope})),1,0)"
    
    calc_tool = saga_detector.grid_calculus / 'Grid Calculator'
    calc_tool.execute(
        verbose=False,
        grids=str(preprocessed_data['slope']),
        result=str(slope_mask),
        formula=formula_slope,
        type=3
    )
    
    print("\nApplying other terrain criteria...")
    
    curv_mask = output_dir / "curvature_mask.sdat"
    calc_tool.execute(
        verbose=False,
        grids=str(preprocessed_data['plan_curvature']),
        result=str(curv_mask),
        formula="ifelse(lt(g1,-0.01),1,0)",
        type=3
    )
    
    rough_mask = output_dir / "roughness_mask.sdat"
    tri_norm = output_dir / "tri_normalized.sdat"
    
    standardize = saga_detector.grid_calculus / 'Grid Standardization'
    standardize.execute(
        verbose=False,
        input=str(preprocessed_data['tri']),
        output=str(tri_norm),
        stretch=1
    )
    
    calc_tool.execute(
        verbose=False,
        grids=str(tri_norm),
        result=str(rough_mask),
        formula="ifelse(gt(g1,0.3),1,0)",
        type=3
    )
    
    veg_height = output_dir / "veg_height.sdat"
    calc_tool.execute(
        verbose=False,
        grids=f"{dsm_path};{dtm_path}",
        result=str(veg_height),
        formula="g1-g2",
        type=9
    )
    
    bare_mask = output_dir / "bare_mask.sdat"
    calc_tool.execute(
        verbose=False,
        grids=str(veg_height),
        result=str(bare_mask),
        formula="ifelse(lt(g1,2.0),1,0)",
        type=3
    )
    
    print("\nCombining all criteria...")
    
    release_raster = output_dir / "rainfall_release_areas.sdat"
    calc_tool.execute(
        verbose=False,
        grids=f"{slope_mask};{curv_mask};{rough_mask};{bare_mask}",
        result=str(release_raster),
        formula="g1*g2*g3*g4",
        type=3
    )
    
    print("\nCalculating statistics...")
    
    import rasterio
    with rasterio.open(release_raster) as src:
        data = src.read(1)
        cellsize = src.transform[0]
        release_pixels = np.sum(data == 1)
        area_m2 = release_pixels * (cellsize ** 2)
        area_km2 = area_m2 / 1e6
    
    print(f"\nRelease Area Results:")
    print(f"  Total area: {area_km2:.3f} km² ({area_m2:.0f} m²)")
    print(f"  Pixels: {release_pixels}")
    print(f"  Output: {release_raster}")
    
    print(f"\nRainfall context:")
    print(f"  Intensity: {assessment['intensity_mmh']:.1f} mm/h")
    print(f"  Antecedent (7d): {assessment['antecedent_7d_mm']:.1f} mm")
    print(f"  Saturation: {saturation:.2f}")
    
    print("\nRelease areas computed successfully")
    print("Next step: Use as input for GPP debris flow simulation")


if __name__ == "__main__":
    compute_rainfall_adjusted_release_areas()