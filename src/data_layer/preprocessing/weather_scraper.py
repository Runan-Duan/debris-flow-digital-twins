from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from datetime import datetime, timedelta
import re
import time
from config.logging import setup_logging

logger = setup_logging()


class AWEKASScraper:
    
    def __init__(self, station_id="34362", headless=True):
        self.station_id = station_id
        self.base_url = "https://stationsweb.awekas.at"
        self.urls = {
            'index-tab': f"{self.base_url}/en/{station_id}/index-tab",
            'table': f"{self.base_url}/en/{station_id}/table",
            'data': f"{self.base_url}/en/{station_id}/data",
            'statistic': f"{self.base_url}/en/{station_id}/statistic"
        }
        
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 15)
    
    def scrape_one_day(self):
        date_element = self.wait.until(
            EC.visibility_of_element_located(
                (By.XPATH, "//div[contains(@class,'date') and contains(@class,'visible')]//ion-text")
            )
        )
        date_text = date_element.text
        date_obj = datetime.strptime(date_text, "%B %d, %Y").date()
        
        # table = self.wait.until(
        #     EC.visibility_of_element_located(
        #         (By.XPATH, "//div[contains(@class,'card') and contains(@class,'visible')]//table")
        #     )
        # )
        
        # rows = table.find_elements(By.XPATH, ".//tbody/tr")
        rows_count = len(
            self.driver.find_elements(
                By.XPATH,
                "//div[contains(@class,'card') and contains(@class,'visible')]//tbody/tr"
            )
        )

        day_data = []
        
        # for row in rows:
        for i in range(rows_count):
            rows = self.driver.find_elements(
                By.XPATH,
                "//div[contains(@class,'card') and contains(@class,'visible')]//tbody/tr"
            )

            row = rows[i]
            cells = row.find_elements(By.TAG_NAME, "td")
            
            def clean_num(text):
                clean = re.sub(r"[^\d\.\-]", "", text)
                return float(clean) if clean else 0.0
            
            time_str = cells[0].text.strip()

            # Skip summary rows (max / min / avg)
            if not re.match(r"^\d{2}:\d{2}$", time_str):
                continue

            timestamp = datetime.combine(
                date_obj,
                datetime.strptime(time_str, "%H:%M").time()
            )
            
            day_data.append({
                "timestamp": timestamp,
                "precipitation_mm": clean_num(cells[6].text),
                "temperature_c": clean_num(cells[1].text),
                "humidity_percent": clean_num(cells[2].text),
                "pressure_hpa": clean_num(cells[3].text),
                "wind_kmh": clean_num(cells[4].text)
            })
        
        return date_obj, day_data
    
    def go_to_previous_day(self, current_date_text):
        prev_button = self.wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[contains(@class,'date') and contains(@class,'visible')]//ion-buttons[contains(@class,'left')]//ion-button")
            )
        )
        
        prev_button.click()
        
        self.wait.until(
            lambda d: d.find_element(
                By.XPATH,
                "//div[contains(@class,'date') and contains(@class,'visible')]//ion-text"
            ).text != current_date_text
        )
    
    def scrape_multiple_days(self, days=14):
        self.driver.get(self.urls['table'])
        self.accept_cookies_ionic(self.driver)
        
        all_data = []
        dates_collected = []
        
        for i in range(days):
            date_element = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//div[contains(@class,'date') and contains(@class,'visible')]//ion-text")
                )
            )
            current_date_text = date_element.text
            
            date_obj, day_data = self.scrape_one_day()
            logger.info(f"Collect data from {date_obj}")
            dates_collected.append(date_obj)
            all_data.extend(day_data)
            
            if i < (days - 1):
                self.go_to_previous_day(current_date_text)
        
        return all_data, dates_collected
    
    def close(self):
        self.driver.quit()

    def accept_cookies_ionic(self,driver, timeout=10):
        try:
            # Wait until ion-modal is present
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script(
                    "return document.querySelector('ion-modal#cookie-banner') !== null"
                )
            )

            # Click "Accept all" inside Shadow DOM
            driver.execute_script("""
                const modal = document.querySelector('ion-modal#cookie-banner');
                if (!modal) return;

                const root = modal.shadowRoot;
                if (!root) return;

                const buttons = modal.querySelectorAll('ion-button');
                for (const btn of buttons) {
                    if (btn.innerText.trim().toLowerCase().includes('accept')) {
                        btn.click();
                        return;
                    }
                }
            """)
            logger.info("Cookie banner accepted")

            # Small wait to allow modal to close
            time.sleep(0.5)

        except TimeoutException:
            logger.info("No cookie banner found")
