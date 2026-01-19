import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_layer.storage.weather_repository import WeatherRepository
from src.models.risk.risk_calculator import RiskCalculator
import subprocess
import time


def check_requirements():
    print("Checking system requirements...")
    
    errors = []
    
    # Check database config
    try:
        from config.database import load_config
        config = load_config()
        print(f"  Database config: OK ({config.get('database')})")
    except Exception as e:
        errors.append(f"Database config: {e}")
    
    # Check data files
    release_raster = Path("data/processed/release_areas/release_areas.sdat")
    if release_raster.exists():
        print(f"  Release areas: OK ({release_raster})")
    else:
        errors.append(f"Release areas not found: {release_raster}")
    
    sim_sdat = Path("data/processed/simulations/sim_003/process_area.sdat")
    if sim_sdat.exists():
        print(f"  Simulation data: OK ({sim_sdat})")
    else:
        errors.append(f"Simulation data not found: {sim_sdat}")
    
    if errors:
        print("\nErrors found:")
        for err in errors:
            print(f"  - {err}")
        return False
    
    print("\nAll checks passed!")
    return True


def setup_database():
    print("\nSetting up database...")
    
    try:
        repo = WeatherRepository()
        repo.create_table()
        print("  Database tables created")
        return True
    except Exception as e:
        print(f"  Error: {e}")
        return False


def collect_initial_data():
    print("\nCollecting initial weather data...")
    print("This will scrape 14 days of data from AWEKAS (may take 1-2 minutes)")
    
    response = input("Proceed? (y/n): ")
    if response.lower() != 'y':
        print("Skipped")
        return False
    
    try:
        from src.data_layer.preprocessing.weather_scraper import AWEKASScraper
        
        scraper = AWEKASScraper(station_id="34362", headless=True)
        all_data, dates = scraper.scrape_multiple_days(days=14)
        scraper.close()
        
        repo = WeatherRepository()
        repo.insert_batch(all_data)
        
        print(f"  Collected {len(all_data)} records from {len(dates)} days")
        return True
    except Exception as e:
        print(f"  Error: {e}")
        return False


def test_risk_calculation():
    print("\nTesting risk calculation...")
    
    try:
        risk_calc = RiskCalculator()
        assessment = risk_calc.calculate_current_risk()
        
        print(f"  Risk Level: {assessment['risk_level']}")
        print(f"  Exceedance: {assessment['exceedance_ratio']:.2f}")
        print(f"  Saturation: {assessment['saturation']:.2f}")
        return True
    except Exception as e:
        print(f"  Error: {e}")
        return False


def start_api():
    print("\nStarting API server...")
    print("Press Ctrl+C to stop\n")
    
    try:
        subprocess.run([
            "uvicorn",
            "src.api.main:app",
            "--reload",
            "--host", "0.0.0.0",
            "--port", "8000"
        ])
    except KeyboardInterrupt:
        print("\nAPI server stopped")


def main():
    print("DEBRIS FLOW DIGITAL TWIN - QUICK START")
    
    if not check_requirements():
        print("\nFix errors above before proceeding")
        return
    
    if not setup_database():
        print("\nDatabase setup failed")
        return
    
    has_data = False
    try:
        repo = WeatherRepository()
        recent = repo.get_recent_data(hours=24)
        if recent:
            print(f"\nFound {len(recent)} existing weather records")
            has_data = True
    except:
        pass
    
    if not has_data:
        if not collect_initial_data():
            print("\nWarning: No weather data available")
            print("Risk calculations will not work without data")
    
    if has_data or collect_initial_data():
        test_risk_calculation()
    
    print("SETUP COMPLETE")
    print("\nNext steps:")
    print("1. Start API server:")
    print("   python -m uvicorn src.api.main:app --reload")
    print("\n2. Open frontend:")
    print("   Open frontend/index.html in your browser")
    print("\n3. Or use Python HTTP server:")
    print("   cd frontend && python -m http.server 8080")
    
    response = input("\nStart API server now? (y/n): ")
    if response.lower() == 'y':
        start_api()


if __name__ == "__main__":
    main()