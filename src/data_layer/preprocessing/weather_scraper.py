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
        chrome_options.add_argument(
            'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        )

        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 15)

    # Unit conversion helpers

    @staticmethod
    def f_to_c(f):
        return (f - 32) * 5.0 / 9.0

    @staticmethod
    def inhg_to_hpa(inhg):
        return inhg * 33.8639

    @staticmethod
    def mph_to_kmh(mph):
        return mph * 1.60934

    @staticmethod
    def inch_to_mm(inch):
        return inch * 25.4

    # Scraping logic

    def scrape_one_day(self):
        date_element = self.wait.until(
            EC.visibility_of_element_located(
                (By.XPATH, "//div[contains(@class,'date') and contains(@class,'visible')]//ion-text")
            )
        )

        date_text = date_element.text
        date_obj = datetime.strptime(date_text, "%B %d, %Y").date()

        rows = self.driver.find_elements(
            By.XPATH,
            "//div[contains(@class,'card') and contains(@class,'visible')]//tbody/tr"
        )

        def clean_num(text):
            clean = re.sub(r"[^\d\.\-]", "", text)
            return float(clean) if clean else 0.0

        day_data = []

        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")

            time_str = cells[0].text.strip()
            if not re.match(r"^\d{2}:\d{2}$", time_str):
                continue

            timestamp = datetime.combine(
                date_obj,
                datetime.strptime(time_str, "%H:%M").time()
            )

            # Raw scraped values (AWEKAS units)
            temp_f = clean_num(cells[1].text)
            humidity = clean_num(cells[2].text)
            pressure_inhg = clean_num(cells[3].text)
            wind_mph = clean_num(cells[4].text)
            precip_in = clean_num(cells[6].text)

            # Unit conversions
            day_data.append({
                "timestamp": timestamp,
                "temperature_c": self.f_to_c(temp_f),
                "humidity_percent": humidity,
                "pressure_hpa": self.inhg_to_hpa(pressure_inhg),
                "wind_kmh": self.mph_to_kmh(wind_mph),
                "precipitation_mm": self.inch_to_mm(precip_in)
            })

        return date_obj, day_data

    def go_to_previous_day(self, current_date_text):
        prev_button = self.wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[contains(@class,'date') and contains(@class,'visible')]"
                           "//ion-buttons[contains(@class,'left')]//ion-button")
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
            logger.info(f"Collected data for {date_obj}")

            dates_collected.append(date_obj)
            all_data.extend(day_data)

            if i < days - 1:
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
