import pandas as pd
import re, time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==================== CONFIG ====================
INPUT_CSV = "data/bronze/acra/acra_data.csv"
OUTPUT_CSV = "data/bronze/companies_sg/companies_sg_data.csv"
MAX_ROWS = 10000
WAIT_TIMEOUT = 20
SLEEP_AFTER_LOAD = 2
SAVE_INTERVAL = 500    # ⬅️ save after every 500 rows
# =================================================

# --- Load & clean data ---
df = pd.read_csv(INPUT_CSV, dtype=str, keep_default_na=False)
print(f"✅ Loaded {len(df)} records from {INPUT_CSV}")
df = df.head(MAX_ROWS)

# --- Setup Chrome ---
options = Options()
options.add_argument("--start-maximized")
# options.add_argument("--headless=new")  # uncomment for headless
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)
driver = webdriver.Chrome(options=options)

# --- Helper function ---
def extract_text(soup, label):
    """Finds a label span and returns the next <label> text."""
    el = soup.find("span", string=re.compile(label, re.I))
    if el:
        val = el.find_next("label")
        if val:
            return val.get_text(strip=True)
    return None

results = []

for i, row in df.iterrows():
    uen = row.get("uen", "").strip()
    name = row.get("entity_name", "").strip()

    if not uen or not name:
        continue

    # Normalize for URL
    name_url = name.replace(".", "").replace(" ", "-")
    url = f"https://www.companies.sg/business/{uen}/{name_url}-"
    print(f"[{i+1}] Fetching: {url}")

    try:
        driver.get(url)
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
        )
        time.sleep(SLEEP_AFTER_LOAD)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        data = {
            "Entity Name": extract_text(soup, "Entity Name"),
            "UEN": extract_text(soup, "UEN"),
            "Registration Incorporation Date": extract_text(soup, "Registration Incorporation Date"),
            "Company Type Description": extract_text(soup, "Company Type Description"),
            "Entity Status Description": extract_text(soup, "Entity Status Description"),
            "Entity Type Description": extract_text(soup, "Entity Type Description"),
            "URL": url,
        }

        results.append(data)
        print(f"✅ Parsed: {data['Entity Name'] or 'N/A'}")

    except Exception as e:
        print(f"⚠️ Error fetching {uen}: {e}")
        results.append({
            "Entity Name": None,
            "UEN": uen,
            "Registration Incorporation Date": None,
            "Company Type Description": None,
            "Entity Status Description": None,
            "Entity Type Description": None,
            "URL": url,
        })

    # Save every 500 rows
    if (i + 1) % SAVE_INTERVAL == 0:
        pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
        print(f"💾 Saved {len(results)} records so far → {OUTPUT_CSV}")

driver.quit()

# Final save
pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
print(f"\n🎯 Completed scraping {len(results)} companies → {OUTPUT_CSV}")
