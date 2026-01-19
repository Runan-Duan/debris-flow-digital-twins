import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.models.risk.risk_calculator import RiskCalculator
from datetime import datetime


def main():
    print("Calculating debris flow risk from rainfall data")
    
    risk_calc = RiskCalculator(
        alpha=14.0,
        beta=0.4,
        field_capacity_mm=100.0,
        base_critical_slope_deg=35.0
    )
    
    assessment = risk_calc.calculate_current_risk()
    
    print(f"\nRisk Assessment ({assessment['timestamp'].strftime('%Y-%m-%d %H:%M')})")
    print(f"Risk Level: {assessment['risk_level']}")
    print(f"Exceedance Ratio: {assessment['exceedance_ratio']:.2f}")
    
    print(f"\nRainfall Metrics:")
    print(f"  Current intensity: {assessment['intensity_mmh']:.1f} mm/h")
    print(f"  Duration: {assessment['duration_h']:.1f} hours")
    print(f"  Threshold intensity: {assessment['threshold_intensity_mmh']:.1f} mm/h")
    print(f"  Total event: {assessment['total_event_mm']:.1f} mm")
    
    print(f"\nAntecedent Rainfall:")
    print(f"  7-day: {assessment['antecedent_7d_mm']:.1f} mm")
    print(f"  14-day: {assessment['antecedent_14d_mm']:.1f} mm")
    print(f"  Effective: {assessment['effective_antecedent_mm']:.1f} mm")
    
    print(f"\nSoil Conditions:")
    print(f"  Saturation index: {assessment['saturation']:.2f}")
    print(f"  Critical slope angle: {assessment['critical_slope_deg']:.1f} degrees")
    
    should_simulate = risk_calc.should_trigger_simulation(assessment)
    
    print(f"\nSimulation Trigger: {'YES' if should_simulate else 'NO'}")
    
    if should_simulate:
        print("Conditions warrant debris flow simulation")
        print("Next step: Run release area detection and GPP simulation")


if __name__ == "__main__":
    main()