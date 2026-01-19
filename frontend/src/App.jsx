import React, { useState, useEffect } from 'react';
import MapView from './components/MapView/MapView';
import WeatherPanel from './components/WeatherPanel/WeatherPanel';
import RiskIndicator from './components/RiskIndicator/RiskIndicator';
import './styles/main.css';

const API_BASE = 'http://localhost:8000';

function App() {
  const [riskData, setRiskData] = useState(null);
  const [weatherData, setWeatherData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 300000); // Update every 5 minutes
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    try {
      const [riskRes, weatherRes] = await Promise.all([
        fetch(`${API_BASE}/risk/current`),
        fetch(`${API_BASE}/weather/latest`)
      ]);

      const risk = await riskRes.json();
      const weather = await weatherRes.json();

      setRiskData(risk.risk_assessment);
      setWeatherData(weather.data || []);
      setLoading(false);
    } catch (error) {
      console.error('Error fetching data:', error);
      setLoading(false);
    }
  };

  const handleScrapeData = async () => {
    try {
      setLoading(true);
      await fetch(`${API_BASE}/weather/scrape?days=1`, { method: 'POST' });
      await fetchData();
    } catch (error) {
      console.error('Error scraping data:', error);
      setLoading(false);
    }
  };

  if (loading) {
    return <div className="loading">Loading...</div>;
  }

  return (
    <div className="app-container">
      <div className="sidebar">
        <h1>Debris Flow Monitor</h1>
        <RiskIndicator riskData={riskData} />
        <WeatherPanel weatherData={weatherData} />
        <button onClick={handleScrapeData} className="scrape-btn">
          Update Weather Data
        </button>
      </div>
      <div className="map-container">
        <MapView riskLevel={riskData?.risk_level} />
      </div>
    </div>
  );
}

export default App;