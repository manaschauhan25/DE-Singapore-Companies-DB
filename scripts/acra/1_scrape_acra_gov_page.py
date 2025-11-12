from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup

def scrape_with_selenium(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)

    # Wait until dataset info has loaded (look for "ACRA" or "datasetId" in page source)
    WebDriverWait(driver, 20).until(
        lambda d: "datasetId" in d.page_source
    )

    # Optional: wait a bit more to ensure all data loads
    time.sleep(5)

    html_content = driver.page_source
    with open("data/bronze/acra/html/rendered_acra_gov.html", "w", encoding="utf-8") as f:
        f.write(html_content)

    driver.quit()
    print("✅ Saved full rendered page with dataset JSON")

# Usage
if __name__ == "__main__":
    scrape_with_selenium("https://data.gov.sg/collections/2/view")
