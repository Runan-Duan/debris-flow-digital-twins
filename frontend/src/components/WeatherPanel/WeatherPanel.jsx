import React from 'react';

const WeatherPanel = ({ weatherData }) => {
  if (!weatherData || weatherData.length === 0) {
    return <div className="weather-panel">No weather data available</div>;
  }

  const latest = weatherData[0];
  const last24h = weatherData.slice(0, 24);
  const totalPrecip = last24h.reduce((sum, d) => sum + (d.precipitation_mm || 0), 0);

  return (
    <div className="weather-panel">
      <h3>Current Weather</h3>
      
      <div className="weather-current">
        <div className="weather-item">
          <label>Temperature:</label>
          <span>{latest.temperature_c?.toFixed(1) || 'N/A'}°C</span>
        </div>

        <div className="weather-item">
          <label>Humidity:</label>
          <span>{latest.humidity_percent?.toFixed(0) || 'N/A'}%</span>
        </div>

        <div className="weather-item">
          <label>Last Hour Rain:</label>
          <span>{latest.precipitation_mm?.toFixed(1) || '0.0'} mm</span>
        </div>

        <div className="weather-item">
          <label>24h Total:</label>
          <span>{totalPrecip.toFixed(1)} mm</span>
        </div>
      </div>

      <h4>Last 24 Hours Precipitation</h4>
      <div className="rainfall-chart">
        {last24h.map((d, i) => (
          <div 
            key={i} 
            className="rainfall-bar"
            style={{ 
              height: `${Math.max(d.precipitation_mm * 10, 2)}px`,
              backgroundColor: d.precipitation_mm > 0 ? '#2196F3' : '#E0E0E0'
            }}
            title={`${new Date(d.timestamp).toLocaleTimeString()}: ${d.precipitation_mm}mm`}
          />
        ))}
      </div>
    </div>
  );
};

export default WeatherPanel;