from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta
from src.data_layer.storage.weather_repository import WeatherRepository
from src.data_layer.preprocessing.weather_scraper import AWEKASScraper

router = APIRouter(prefix="/weather", tags=["weather"])

weather_repo = WeatherRepository()


@router.get("/latest")
async def get_latest_weather():
    """Get latest 24 hours of weather data"""
    try:
        data = weather_repo.get_recent_data(hours=24)
        return {
            "status": "success",
            "count": len(data),
            "data": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/antecedent")
async def get_antecedent_rainfall(days: int = 7):
    """Calculate antecedent rainfall for specified days"""
    try:
        total = weather_repo.get_antecedent_rainfall(days_back=days)
        effective = weather_repo.get_effective_antecedent_rainfall(days_back=days)
        
        return {
            "status": "success",
            "days_back": days,
            "total_mm": total,
            "effective_mm": effective
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/current-event")
async def get_current_event_metrics():
    """Get metrics for current rainfall event"""
    try:
        metrics = weather_repo.get_current_event_metrics()
        
        if not metrics:
            return {
                "status": "success",
                "message": "No active rainfall event"
            }
        
        return {
            "status": "success",
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scrape")
async def scrape_weather_data(days: int = 1):
    """Scrape weather data from AWEKAS station"""
    try:
        scraper = AWEKASScraper(station_id="34362", headless=True)
        
        all_data, dates = scraper.scrape_multiple_days(days=days)
        scraper.close()
        
        weather_repo.insert_batch(all_data)
        
        return {
            "status": "success",
            "days_scraped": len(dates),
            "records_inserted": len(all_data),
            "dates": [str(d) for d in dates]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))