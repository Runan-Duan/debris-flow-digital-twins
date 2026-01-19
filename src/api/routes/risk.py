from fastapi import APIRouter, HTTPException
from src.models.risk.risk_calculator import RiskCalculator

router = APIRouter(prefix="/risk", tags=["risk"])

risk_calc = RiskCalculator(
    alpha=14.0,
    beta=0.4,
    field_capacity_mm=100.0,
    base_critical_slope_deg=35.0
)


@router.get("/current")
async def get_current_risk():
    """Calculate current debris flow risk level"""
    try:
        assessment = risk_calc.calculate_current_risk()
        
        return {
            "status": "success",
            "risk_assessment": assessment
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/should-simulate")
async def should_trigger_simulation():
    """Check if conditions warrant debris flow simulation"""
    try:
        assessment = risk_calc.calculate_current_risk()
        should_run = risk_calc.should_trigger_simulation(assessment)
        
        return {
            "status": "success",
            "should_simulate": should_run,
            "risk_assessment": assessment,
            "reason": _get_trigger_reason(assessment, should_run)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _get_trigger_reason(assessment, should_run):
    if not should_run:
        return "Risk level below simulation threshold"
    
    reasons = []
    
    if assessment['risk_level'] in ['HIGH', 'CRITICAL']:
        reasons.append(f"Risk level: {assessment['risk_level']}")
    
    if assessment['exceedance_ratio'] >= 1.0:
        reasons.append(f"I-D threshold exceeded ({assessment['exceedance_ratio']:.2f})")
    
    if assessment.get('saturation', 0) >= 0.7:
        reasons.append(f"High soil saturation ({assessment['saturation']:.2f})")
    
    return "; ".join(reasons)