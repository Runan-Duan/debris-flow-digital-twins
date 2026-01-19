from src.data_layer.storage.weather_repository import WeatherRepository
from src.models.rainfall.rainfall_threshold import RainfallThreshold, SoilSaturationModel
from datetime import datetime


class RiskCalculator:
    """
    Calculate debris flow risk level based on current rainfall conditions
    Integrates I-D threshold with soil saturation
    """
    
    def __init__(self, 
                 alpha=14.0, 
                 beta=0.4,
                 field_capacity_mm=100.0,
                 base_critical_slope_deg=35.0):
        
        self.weather_repo = WeatherRepository()
        self.threshold = RainfallThreshold(alpha, beta)
        self.saturation_model = SoilSaturationModel(field_capacity_mm)
        self.base_critical_slope = base_critical_slope_deg
    
    def calculate_current_risk(self, target_date=None):
        """
        Calculate risk level for current conditions
        
        Returns:
            dict with:
                - risk_level: LOW, MODERATE, HIGH, CRITICAL
                - exceedance_ratio: float
                - intensity_mmh: float
                - duration_h: float
                - antecedent_7d_mm: float
                - antecedent_14d_mm: float
                - saturation: float
                - critical_slope_deg: float
        """
        if target_date is None:
            target_date = datetime.now()
        
        event_metrics = self.weather_repo.get_current_event_metrics(target_date)
        
        if not event_metrics:
            return {
                'risk_level': 'LOW',
                'exceedance_ratio': 0.0,
                'message': 'No recent rainfall data'
            }
        
        antecedent_7d = self.weather_repo.get_antecedent_rainfall(target_date, days_back=7)
        antecedent_14d = self.weather_repo.get_antecedent_rainfall(target_date, days_back=14)
        effective_antecedent = self.weather_repo.get_effective_antecedent_rainfall(target_date)
        
        intensity = event_metrics['max_intensity_mmh']
        duration = event_metrics['duration_h']
        total_event = event_metrics['total_mm']
        
        risk_level, exceedance = self.threshold.assess_risk_level(
            intensity, duration, antecedent_7d
        )
        
        saturation = self.saturation_model.calculate_saturation(
            total_event, antecedent_7d
        )
        
        critical_slope = self.saturation_model.calculate_critical_slope(
            saturation, self.base_critical_slope
        )
        
        return {
            'risk_level': risk_level,
            'exceedance_ratio': exceedance,
            'intensity_mmh': intensity,
            'duration_h': duration,
            'antecedent_7d_mm': antecedent_7d,
            'antecedent_14d_mm': antecedent_14d,
            'effective_antecedent_mm': effective_antecedent,
            'total_event_mm': total_event,
            'saturation': saturation,
            'critical_slope_deg': critical_slope,
            'threshold_intensity_mmh': self.threshold.get_threshold_intensity(duration),
            'timestamp': target_date
        }
    
    def should_trigger_simulation(self, risk_assessment=None):
        """
        Determine if debris flow simulation should be triggered
        
        Trigger conditions:
        - Risk level >= HIGH
        - OR exceedance ratio >= 1.0
        - OR saturation >= 0.7
        """
        if risk_assessment is None:
            risk_assessment = self.calculate_current_risk()
        
        if risk_assessment['risk_level'] in ['HIGH', 'CRITICAL']:
            return True
        
        if risk_assessment['exceedance_ratio'] >= 1.0:
            return True
        
        if risk_assessment.get('saturation', 0) >= 0.7:
            return True
        
        return False