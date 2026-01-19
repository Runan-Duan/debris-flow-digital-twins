import numpy as np


class RainfallThreshold:
    """
    Rainfall Intensity-Duration threshold for debris flow triggering
    
    Theory: I = alpha * D^(-beta)
    - I: rainfall intensity (mm/h)
    - D: duration (hours)
    - alpha: scaling coefficient (10-20 for alpine regions)
    - beta: exponent (0.3-0.5)
    
    Reference: Guzzetti et al. (2008), Caine (1980)
    """
    
    def __init__(self, alpha=14.0, beta=0.4):
        self.alpha = alpha
        self.beta = beta
    
    def get_threshold_intensity(self, duration_h):
        if duration_h <= 0:
            return float('inf')
        return self.alpha * (duration_h ** (-self.beta))
    
    def calculate_exceedance_ratio(self, intensity_mmh, duration_h, antecedent_mm=0):
        threshold_intensity = self.get_threshold_intensity(duration_h)
        
        exceedance = intensity_mmh / threshold_intensity
        
        if antecedent_mm > 50:
            exceedance *= 1.2
        elif antecedent_mm > 100:
            exceedance *= 1.4
        
        return exceedance
    
    def assess_risk_level(self, intensity_mmh, duration_h, antecedent_mm=0):
        exceedance = self.calculate_exceedance_ratio(intensity_mmh, duration_h, antecedent_mm)
        
        if exceedance >= 1.5:
            return "CRITICAL", exceedance
        elif exceedance >= 1.0:
            return "HIGH", exceedance
        elif exceedance >= 0.7:
            return "MODERATE", exceedance
        else:
            return "LOW", exceedance
    
    def calibrate_from_events(self, historical_events):
        """
        Calibrate alpha and beta from historical debris flow events
        
        Args:
            historical_events: list of dicts with keys:
                - duration_h
                - max_intensity_mmh
                - debris_flow_occurred (bool)
        """
        from scipy.optimize import curve_fit
        
        events = [e for e in historical_events if e['debris_flow_occurred']]
        
        if len(events) < 3:
            return self.alpha, self.beta
        
        durations = np.array([e['duration_h'] for e in events])
        intensities = np.array([e['max_intensity_mmh'] for e in events])
        
        def power_law(d, alpha, beta):
            return alpha * d ** (-beta)
        
        try:
            params, _ = curve_fit(
                power_law,
                durations,
                intensities,
                p0=[14.0, 0.4],
                bounds=([1.0, 0.1], [100.0, 1.0])
            )
            
            self.alpha, self.beta = params
            return self.alpha, self.beta
        except:
            return self.alpha, self.beta


class SoilSaturationModel:
    """
    Soil saturation calculation from rainfall
    
    Theory: Saturation affects pore pressure and reduces effective stress
    m = min[(Event_rain + Antecedent_rain) / Field_capacity, 1.0]
    
    Reference: Berti et al. (2020), Godt et al. (2012)
    """
    
    def __init__(self, field_capacity_mm=100.0):
        self.field_capacity_mm = field_capacity_mm
    
    def calculate_saturation(self, event_rainfall_mm, antecedent_rainfall_mm):
        total_water = event_rainfall_mm + antecedent_rainfall_mm
        saturation = min(total_water / self.field_capacity_mm, 1.0)
        return saturation
    
    def calculate_critical_slope(self, saturation, base_slope_deg=35.0, reduction_factor=0.2):
        """
        Adjust critical slope angle based on saturation
        
        Theory: theta_crit(wet) = theta_crit(dry) * (1 - reduction_factor * saturation)
        
        Reference: Kean et al. (2011), Thomas et al. (2023)
        """
        critical_slope = base_slope_deg * (1 - reduction_factor * saturation)
        return critical_slope


class InfiniteSlopeStability:
    """
    Infinite slope stability with pore pressure
    
    Theory: FS = [c' + (gamma*z - m*gamma_w*z)*cos²(theta)*tan(phi')] / [gamma*z*sin(theta)*cos(theta)]
    
    Reference: Montgomery & Dietrich (1994), Baum et al. (2008) - TRIGRS
    """
    
    def __init__(self, cohesion_kpa=5.0, friction_angle_deg=35.0,
                 soil_unit_weight_knm3=18.0, soil_depth_m=1.0):
        self.cohesion = cohesion_kpa
        self.friction_angle = np.radians(friction_angle_deg)
        self.gamma_soil = soil_unit_weight_knm3
        self.gamma_water = 9.81
        self.soil_depth = soil_depth_m
    
    def calculate_factor_of_safety(self, slope_deg, saturation):
        theta = np.radians(slope_deg)
        
        effective_weight = (self.gamma_soil * self.soil_depth - 
                          saturation * self.gamma_water * self.soil_depth)
        
        numerator = (self.cohesion + 
                    effective_weight * np.cos(theta)**2 * np.tan(self.friction_angle))
        
        denominator = self.gamma_soil * self.soil_depth * np.sin(theta) * np.cos(theta)
        
        if denominator > 0:
            fs = numerator / denominator
        else:
            fs = 999.0
        
        return fs
    
    def is_unstable(self, slope_deg, saturation):
        fs = self.calculate_factor_of_safety(slope_deg, saturation)
        return fs < 1.0