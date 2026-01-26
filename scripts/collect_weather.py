import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_layer.preprocessing.weather_scraper import AWEKASScraper
from src.data_layer.storage.weather_repository import WeatherRepository


def main():
    print("Collecting weather data from AWEKAS station 34362")
    
    repo = WeatherRepository()
    repo.create_table()
    
    scraper = AWEKASScraper(station_id="34362", headless=True)
    
    days_to_collect = 14
    print(f"Scraping last {days_to_collect} days...")
    
    all_data, dates = scraper.scrape_multiple_days(days=days_to_collect)
    scraper.close()
    
    print(f"Collected {len(all_data)} records from {len(dates)} days")
    print(f"Date range: {dates[-1]} to {dates[0]}")
    
    repo.insert_batch(all_data)
    
    print("Data stored in database")
    
    antecedent_7d = repo.get_antecedent_rainfall(days_back=7)
    antecedent_14d = repo.get_antecedent_rainfall(days_back=14)
    effective = repo.get_effective_antecedent_rainfall(days_back=14)
    
    print(f"\nAntecedent rainfall:")
    print(f"  7-day: {antecedent_7d:.1f} mm")
    print(f"  14-day: {antecedent_14d:.1f} mm")
    print(f"  Effective: {effective:.1f} mm")


if __name__ == "__main__":
    main()