"""
RecordOwl Website Scraper
Simple, clean, human-like scraping to avoid blocks
"""

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc
import time
import random
import os

class RecordOwlScraper:
    # def __init__(self, headless=True):
    #     print("Setting up browser...")
        
    #     options = Options()
        
    #     if headless:
    #         options.add_argument("--headless=new")
    #     options.add_argument("--no-sandbox")
    #     options.add_argument("--disable-dev-shm-usage")
    #     options.add_argument("--disable-gpu")
    #     options.add_argument("--window-size=1920,1080")
    #     options.add_argument("--disable-blink-features=AutomationControlled")
    #     options.add_argument("--disable-infobars")
    #     # if headless:
    #     #     options.add_argument('--headless')
    #     # else:
    #     #     options.add_argument('--headless=new')
        
    #     # # Anti-detection measures
    #     # options.add_argument('--no-sandbox')
    #     # options.add_argument('--disable-dev-shm-usage')
    #     # options.add_argument('--disable-blink-features=AutomationControlled')
    #     # options.add_experimental_option("excludeSwitches", ["enable-automation"])
    #     # options.add_experimental_option('useAutomationExtension', False)
        
    #     # Human-like user agent
    #     options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
    #     service = Service(ChromeDriverManager().install())
    #     self.driver = webdriver.Chrome(service=service, options=options)
        
    #     # Remove webdriver property
    #     self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    #     print("Browser ready!\n")
    def __init__(self, headless=True):
        print("Setting up undetected Chrome browser...")

        # ✅ Use undetected_chromedriver instead of normal webdriver
        options = uc.ChromeOptions()

        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-infobars")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36")

        # ✅ Initialize undetected Chrome (bypasses Cloudflare)
        # self.driver = uc.Chrome(options=options)
        self.driver = uc.Chrome(version_main=141, options=options)


        # Optional: make Selenium less detectable
        self.driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4]});
                """
            },
        )

        print("✅ Undetected Chrome browser ready!\n")

    def search_uen(self, uen):
        """Search for company by UEN and return company page URL"""
        try:
            # Build search URL and open it
            search_url = f"https://recordowl.com/search?name={uen}"
            print(f"🔍 Searching RecordOwl for: {uen}")
            self.driver.get(search_url)

            # Wait for company link to appear (up to 20s)
            try:
                element = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/company/']"))
                )
                company_url = element.get_attribute("href")
                print(f"✅ Found company link for {uen}: {company_url}")
                return company_url

            except Exception:
                # No company link found — save HTML for later inspection
                print(f"⚠️ No company link found for {uen}, saving page for review.")
                os.makedirs("debug_html", exist_ok=True)
                file_path = f"debug_html/{uen}.html"
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
                print(f"💾 Saved HTML → {file_path}")
                return None

        except Exception as e:
            print(f"❌ Error while processing {uen}: {e}")
            return None
    # def search_uen(self, uen):
    #     """Search for company by UEN and return company page URL"""
    #     try:
    #         # Go to RecordOwl search with UEN
    #         search_url = f"https://recordowl.com/search?name={uen}"
            
    #         self.driver.get(search_url)

    #         # Wait until search results table appears
    #         try:
    #             WebDriverWait(self.driver, 20).until(
    #                 EC.presence_of_element_located((By.CSS_SELECTOR, "tbody tr a[href*='/company/']"))
    #             )
    #             print(f"✅ Search results loaded for {uen}")
    #         except Exception:
    #             print(f"⚠️ No search results loaded for {uen} — saving raw HTML anyway")

    #         # Always save HTML for debugging
    #         os.makedirs("debug_html", exist_ok=True)
    #         file_path = f"debug_html/{uen}.html"
    #         with open(file_path, "w", encoding="utf-8") as f:
    #             f.write(self.driver.page_source)
    #         print(f"💾 Saved HTML → {file_path}")
            
    #         # Check if we got results - look for company link
    #         try:
    #             # Find the company link in search results
    #             # RecordOwl shows company links after search
    #             company_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/company/']")
                
    #             if company_links:
    #                 # Get the first company link
    #                 company_url = company_links[0].get_attribute('href')
    #                 return company_url
    #             else:
    #                 return None
                    
    #         except:
    #             return None

                
    #     except Exception as e:
    #         return None
    
    def scrape_company_data(self, company_url, uen, company_name):
        """Scrape company data from RecordOwl company page"""
        try:
            # Already on page or navigate to it
            if self.driver.current_url != company_url:
                self.driver.get(company_url)
                time.sleep(random.uniform(2, 3))
            
            # Initialize data dictionary
            data = {
                'uen': uen,
                'company_name': company_name,
                'recordowl_url': company_url,
                'website': None,
                'phone': None,
                'email': None,
                'address': None,
                'industry': None,
            }
            
            # Try to find website
            try:
                # Look for links that are likely websites
                links = self.driver.find_elements(By.TAG_NAME, "a")
                for link in links:
                    href = link.get_attribute('href')
                    text = link.text.lower()
                    
                    if href and ('http' in href):
                        # Skip internal links
                        if 'recordowl.com' not in href and 'javascript' not in href:
                            # Check if it looks like a company website
                            if any(domain in href.lower() for domain in ['.com', '.sg', '.net', '.org']):
                                # Skip social media
                                if not any(social in href.lower() for social in ['facebook', 'linkedin', 'twitter', 'instagram']):
                                    data['website'] = href
                                    break
            except:
                pass
            
            # Try to find phone
            try:
                page_text = self.driver.find_element(By.TAG_NAME, "body").text
                # Simple phone pattern for Singapore
                import re
                phone_match = re.search(r'(\+65\s?\d{4}\s?\d{4}|\d{4}\s?\d{4})', page_text)
                if phone_match:
                    data['phone'] = phone_match.group(1)
            except:
                pass
            
            # Try to find email
            try:
                page_text = self.driver.find_element(By.TAG_NAME, "body").text
                import re
                email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', page_text)
                if email_match:
                    data['email'] = email_match.group(0)
            except:
                pass
            
            return data
            
        except Exception as e:
            return None
    
    def process_batch(self, df, num_companies=1000, start_from=0):
        """Process multiple companies"""
        print(f"Processing {num_companies} companies starting from {start_from}...")
        print("Using RecordOwl - scraping carefully to avoid blocks\n")
        
        results = []
        df_subset = df.iloc[start_from:start_from + num_companies]
        
        for idx, row in df_subset.iterrows():
            uen = row['uen']
            company_name = row['entity_name']
            
            print(f"[{len(results)+1}/{num_companies}] {company_name[:50]}  -  {uen}")
            
            # Step 1: Search for company
            company_url = self.search_uen(uen)
            
            if company_url:
                # Step 2: Scrape data
                data = self.scrape_company_data(company_url, uen, company_name)
                if data:
                    results.append(data)
                    if data['website']:
                        print(f"  ✓ Found website: {data['website']}")
                    else:
                        print(f"  - No website found")
                else:
                    print(f"  - Could not scrape data")
            else:
                print(f"  - Company not found on RecordOwl")
                results.append({
                    'uen': uen,
                    'company_name': company_name,
                    'recordowl_url': None,
                    'website': None,
                    'phone': None,
                    'email': None,
                    'address': None,
                    'industry': None,
                })
            
            # Progress update
            if len(results) % 25 == 0:
                found_websites = sum(1 for r in results if r['website'])
                print(f"\n--- Progress: {len(results)}/{num_companies} - Websites: {found_websites} ({found_websites/len(results)*100:.1f}%) ---\n")
            
            # Human-like delay between requests (important!)
            time.sleep(random.uniform(3, 6))
        
        return pd.DataFrame(results)
    
    def close(self):
        """Close browser"""
        self.driver.quit()
        print("\nBrowser closed")


# Main execution
if __name__ == "__main__":
    print("="*60)
    print("RecordOwl Scraper")
    print("="*60)
    
    # Configuration
    INPUT_FILE = 'data/acra_filtered_100k.csv'
    OUTPUT_FILE = 'data/websites_recordowl.csv'
    NUM_COMPANIES = 100      # Start small to test
    START_FROM = 0           # Which row to start from
    HEADLESS = True          # Set False to see browser
    
    scraper = None
    
    try:
        # Load data
        print(f"\nLoading: {INPUT_FILE}")
        df = pd.read_csv(INPUT_FILE)
        print(f"Loaded {len(df)} companies\n")
        
        # Initialize scraper
        scraper = RecordOwlScraper(headless=HEADLESS)
        
        # Process companies
        results = scraper.process_batch(df, NUM_COMPANIES, START_FROM)
        
        # Save results
        results.to_csv(OUTPUT_FILE, index=False)
        
        # Summary
        total = len(results)
        found_websites = results['website'].notna().sum()
        found_phones = results['phone'].notna().sum()
        found_emails = results['email'].notna().sum()
        
        print(f"\n{'='*60}")
        print(f"COMPLETED!")
        print(f"{'='*60}")
        print(f"Total processed: {total}")
        print(f"Websites found: {found_websites} ({found_websites/total*100:.1f}%)")
        print(f"Phones found: {found_phones} ({found_phones/total*100:.1f}%)")
        print(f"Emails found: {found_emails} ({found_emails/total*100:.1f}%)")
        print(f"Saved to: {OUTPUT_FILE}")
        print(f"{'='*60}\n")
        
        # Show sample
        print("Sample results (first 10 with websites):")
        sample = results[results['website'].notna()].head(10)
        for _, row in sample.iterrows():
            print(f"  {row['company_name'][:40]:40} → {row['website']}")
        
    except KeyboardInterrupt:
        print("\n\nStopped by user")
        
    finally:
        if scraper:
            scraper.close()