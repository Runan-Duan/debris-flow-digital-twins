import React from 'react';

const RiskIndicator = ({ riskData }) => {
  if (!riskData) {
    return <div className="risk-indicator">Loading risk data...</div>;
  }

  const getRiskColor = (level) => {
    const colors = {
      'LOW': '#4CAF50',
      'MODERATE': '#FFC107',
      'HIGH': '#FF9800',
      'CRITICAL': '#F44336'
    };
    return colors[level] || '#CCCCCC';
  };

  return (
    <div className="risk-indicator">
      <div 
        className="risk-level-badge"
        style={{ backgroundColor: getRiskColor(riskData.risk_level) }}
      >
        {riskData.risk_level}
      </div>

      <div className="risk-metrics">
        <div className="metric">
          <label>Exceedance Ratio:</label>
          <span>{riskData.exceedance_ratio?.toFixed(2) || 'N/A'}</span>
        </div>

        <div className="metric">
          <label>Intensity:</label>
          <span>{riskData.intensity_mmh?.toFixed(1) || 'N/A'} mm/h</span>
        </div>

        <div className="metric">
          <label>Duration:</label>
          <span>{riskData.duration_h?.toFixed(1) || 'N/A'} hours</span>
        </div>

        <div className="metric">
          <label>Antecedent (7d):</label>
          <span>{riskData.antecedent_7d_mm?.toFixed(1) || 'N/A'} mm</span>
        </div>

        <div className="metric">
          <label>Saturation:</label>
          <span>{(riskData.saturation * 100)?.toFixed(0) || 'N/A'}%</span>
        </div>

        <div className="metric">
          <label>Critical Slope:</label>
          <span>{riskData.critical_slope_deg?.toFixed(1) || 'N/A'}°</span>
        </div>
      </div>
    </div>
  );
};

export default RiskIndicator;