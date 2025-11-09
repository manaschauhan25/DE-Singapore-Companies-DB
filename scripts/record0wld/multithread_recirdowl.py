"""
RecordOwl Comprehensive Scraper - MULTITHREADED VERSION
Extracts ALL available data from company pages
"""
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time
import random
import os
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class RecordOwlComprehensiveScraper:
    def __init__(self, headless=False):
        print("Setting up browser...\n")
        self.headless_mode = headless  # Store for restart
        
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Change this to match YOUR Chrome version
        self.driver = uc.Chrome(version_main=142, options=options)
        
        # Make it less detectable
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
        
        print("✅ Browser ready!\n")

    def restart_browser(self):
        """Restart browser to prevent freezing"""
        print("\n🔄 Restarting browser...")
        
        # Try to close existing browser (ignore if already closed)
        try:
            self.driver.quit()
        except:
            pass  # Browser already closed or error - that's fine
        
        # Wait for OS to release resources
        time.sleep(3)
        
        # Create new browser instance
        try:
            options = uc.ChromeOptions()
            if self.headless_mode:
                options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            
            self.driver = uc.Chrome(version_main=142, options=options)
            
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
            
            print("✅ Browser restarted!\n")
            time.sleep(2)
            
        except Exception as e:
            print(f"⚠️  Error restarting browser: {e}")
            print("Trying one more time...")
            time.sleep(5)
            
            # Second attempt
            options = uc.ChromeOptions()
            if self.headless_mode:
                options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            
            self.driver = uc.Chrome(version_main=142, options=options)
            
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
            
            print("✅ Browser restarted on second attempt!\n")
            time.sleep(2)

    def extract_text_by_label(self, label_text):
        """
        Extract value from a dt/dd pair by label text
        """
        try:
            # Find all dt elements
            dts = self.driver.find_elements(By.TAG_NAME, "dt")
            for dt in dts:
                if label_text.lower() in dt.text.lower():
                    # Get the corresponding dd element (next sibling)
                    dd = dt.find_element(By.XPATH, "following-sibling::dd[1]")
                    return dd.text.strip()
        except:
            pass
        return None

    def extract_link_by_label(self, label_text):
        """
        Extract link (href) from a dt/dd pair by label text
        """
        try:
            dts = self.driver.find_elements(By.TAG_NAME, "dt")
            for dt in dts:
                if label_text.lower() in dt.text.lower():
                    dd = dt.find_element(By.XPATH, "following-sibling::dd[1]")
                    link = dd.find_element(By.TAG_NAME, "a")
                    return link.get_attribute("href")
        except:
            pass
        return None

    def extract_social_media_links(self):
        """
        Extract all social media links from the sidebar
        Returns dict with platform names as keys
        """
        social_media = {}
        
        try:
            # Look for social media section
            page_text = self.driver.page_source
            
            # Common social media domains
            social_platforms = {
                'facebook': r'https?://(?:www\.)?facebook\.com/[^\s"<>]+',
                'linkedin': r'https?://(?:www\.)?linkedin\.com/[^\s"<>]+',
                'twitter': r'https?://(?:www\.)?(?:twitter|x)\.com/[^\s"<>]+',
                'instagram': r'https?://(?:www\.)?instagram\.com/[^\s"<>]+',
                'youtube': r'https?://(?:www\.)?youtube\.com/[^\s"<>]+',
                'tiktok': r'https?://(?:www\.)?tiktok\.com/[^\s"<>]+',
                'pinterest': r'https?://(?:www\.)?pinterest\.com/[^\s"<>]+',
            }
            
            for platform, pattern in social_platforms.items():
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                if matches:
                    # Get unique matches and take the first one
                    unique_matches = list(set(matches))
                    social_media[platform] = unique_matches[0]
        except:
            pass
        
        return social_media

    def extract_description(self):
        """
        Extract company description from the Description section
        """
        try:
            # Look for "About [COMPANY NAME]" section
            dts = self.driver.find_elements(By.TAG_NAME, "dt")
            for dt in dts:
                if "description" in dt.text.lower():
                    dd = dt.find_element(By.XPATH, "following-sibling::dd[1]")
                    # Get all paragraphs in the description
                    paragraphs = dd.find_elements(By.TAG_NAME, "p")
                    # First paragraph usually contains the actual description
                    if paragraphs:
                        return paragraphs[0].text.strip()
        except:
            pass
        return None

    def extract_company_founder(self):
        """
        Extract company founder/founding date from timeline
        """
        try:
            # Look for "Company Founded" in timeline
            page_text = self.driver.page_source
            
            # Pattern to find founding date
            if "Company Founded" in page_text:
                # Look for date near "Company Founded"
                pattern = r'Company Founded.*?(\d{1,2}\s+\w+\s+\d{4})'
                match = re.search(pattern, page_text, re.DOTALL)
                if match:
                    return match.group(1)
        except:
            pass
        return None

    def scrape_company(self, uen, company_name, retry=0):
        """
        Scrape comprehensive company data with retry logic
        """
        max_retries = 2
        
        try:
            # Step 1: Search for company
            search_url = f"https://recordowl.com/search?name={uen}"
            
            try:
                self.driver.set_page_load_timeout(30)
                self.driver.get(search_url)
            except Exception as e:
                if retry < max_retries:
                    print(f"    ⚠️ Timeout, retrying... ({retry + 1}/{max_retries})")
                    time.sleep(3)
                    return self.scrape_company(uen, company_name, retry + 1)
                else:
                    print(f"    ❌ Timeout after {max_retries} retries")
                    return None
            
            # Find company link
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/company/']"))
                )
                company_url = element.get_attribute("href")
            except:
                print(f"    ❌ Company not found in search")
                return None
            
            # Step 2: Visit company page
            try:
                self.driver.set_page_load_timeout(30)
                self.driver.get(company_url)
            except Exception as e:
                if retry < max_retries:
                    print(f"    ⚠️ Timeout on company page, retrying...")
                    time.sleep(3)
                    return self.scrape_company(uen, company_name, retry + 1)
                else:
                    print(f"    ❌ Timeout on company page")
                    return None
            
            # Wait for page to load
            time.sleep(random.uniform(2, 3))
            
            # Step 3: Extract all data
            data = {
                'uen': uen,
                'company_name': company_name,
                'company_link': company_url,
                'registration_number': None,
                'registered_address': None,
                'operating_status': None,
                'company_age': None,
                'building': None,
                'contact_number': None,
                'website': None,
                'description': None,
                'primary_ssic_code': None,
                'primary_industry': None,
                'secondary_ssic_code': None,
                'secondary_industry': None,
                'company_founder': None,
                'facebook': None,
                'linkedin': None,
                'twitter': None,
                'instagram': None,
                'youtube': None,
                'tiktok': None,
                'pinterest': None,
            }
            
            # Extract basic information
            data['registration_number'] = self.extract_text_by_label("Registration Number")
            data['registered_address'] = self.extract_text_by_label("Registered Address")
            data['operating_status'] = self.extract_text_by_label("Operating Status")
            data['company_age'] = self.extract_text_by_label("Company Age")
            data['building'] = self.extract_text_by_label("Building")
            data['contact_number'] = self.extract_text_by_label("Contact Number")
            
            # Extract website
            website_link = self.extract_link_by_label("Website")
            if website_link:
                data['website'] = website_link
            
            # Extract description
            data['description'] = self.extract_description()
            
            # Extract SSIC codes and industries
            data['primary_ssic_code'] = self.extract_text_by_label("Primary SSIC Code")
            data['primary_industry'] = self.extract_text_by_label("Primary Industry")
            data['secondary_ssic_code'] = self.extract_text_by_label("Secondary SSIC Code")
            data['secondary_industry'] = self.extract_text_by_label("Secondary Industry")
            
            # Extract company founder/founding date
            data['company_founder'] = self.extract_company_founder()
            
            # Extract social media links
            social_media = self.extract_social_media_links()
            for platform, link in social_media.items():
                if platform in data:
                    data[platform] = link
            
            # Short delay before next request
            time.sleep(random.uniform(1, 2))
            
            return data
            
        except Exception as e:
            if "timeout" in str(e).lower() and retry < max_retries:
                print(f"    ⚠️ Timeout, retrying... ({retry + 1}/{max_retries})")
                time.sleep(3)
                return self.scrape_company(uen, company_name, retry + 1)
            else:
                print(f"    ❌ Error: {str(e)[:50]}")
                return None

    def close(self):
        """Close the browser"""
        try:
            self.driver.quit()
            print("\n✅ Browser closed")
        except:
            pass

    def filter_data(self, df):
        """Apply filters to keep only relevant companies"""
        original = len(df)
        
        # Keep only Live companies
        status_col = next((c for c in df.columns if 'status' in c.lower()), None)
        if status_col:
            df = df[df[status_col].str.contains('Live', case=False, na=False)]
        
        # Keep only Companies and LLPs
        type_col = next((c for c in df.columns if 'entity_type' in c.lower()), None)
        if type_col:
            df = df[df[type_col].str.contains('Company|Partnership', case=False, na=False)]
        
        # Keep only companies registered after 2005
        date_col = next((c for c in df.columns if 'incorporation' in c.lower()), None)
        if date_col:
            df['reg_year'] = pd.to_datetime(df[date_col], errors='coerce').dt.year
            df = df[df['reg_year'] >= 2005]
        
        # Keep only companies with industry code
        ssic_col = next((c for c in df.columns if 'ssic' in c.lower()), None)
        if ssic_col:
            df = df[df[ssic_col].notna()]
        
        print(f"Filtered: {original:,} -> {len(df):,} records")
        return df


# MULTITHREADING WORKER FUNCTION
def worker_thread(thread_id, companies_df, headless, results_lock, results_list, progress_lock, progress_counter):
    """Each worker runs its own browser and scrapes companies"""
    scraper = RecordOwlComprehensiveScraper(headless=headless)
    local_count = 0
    
    try:
        for idx, row in companies_df.iterrows():
            uen = row['uen']
            company_name = row['entity_name']
            local_count += 1
            
            # Scrape
            data = scraper.scrape_company(uen, company_name)
            
            if not data:
                data = {
                    'uen': uen,
                    'company_name': company_name,
                    'company_link': None,
                }
            
            # Thread-safe: add result
            with results_lock:
                results_list.append(data)
            
            # Thread-safe: update progress
            with progress_lock:
                progress_counter[0] += 1
                total_done = progress_counter[0]
                
                # Show progress
                if data.get('website'):
                    print(f"[Thread-{thread_id}] ✅ {company_name[:40]} → {data['website']}")
                elif data.get('contact_number'):
                    print(f"[Thread-{thread_id}] 📞 {company_name[:40]} → {data['contact_number']}")
            
            # Restart browser every 50 per thread
            if local_count % 50 == 0:
                with progress_lock:
                    print(f"\n[Thread-{thread_id}] 🔄 Browser restart (50 scraped)")
                scraper.restart_browser()
        
    finally:
        scraper.close()
        print(f"\n[Thread-{thread_id}] ✅ Finished - scraped {local_count} companies")


# Main execution
if __name__ == "__main__":
    print("="*70)
    print("RecordOwl Comprehensive Scraper - MULTITHREADED")
    print("Extracts ALL available data fields")
    print("="*70)
    print()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Configuration
    INPUT_FILE = 'data/bronze/acra/acra_data.csv'
    OUTPUT_FILE = f'data/bronze/recordowld/websites_recordowl_{timestamp}.csv'
    NUM_COMPANIES = 10000    # Change this as needed
    START_FROM = 4333        # Resume from here if needed
    NUM_THREADS = 3          # Number of parallel browsers
    HEADLESS = False         # False = see browser (recommended)
    
    # Time estimate
    time_estimate = NUM_COMPANIES * 3 / 3600 / NUM_THREADS
    print(f"Configuration:")
    print(f"  Companies: {NUM_COMPANIES}")
    print(f"  Start from: {START_FROM}")
    print(f"  Threads: {NUM_THREADS} (parallel browsers)")
    print(f"  Headless: {HEADLESS}")
    print(f"  Browser restart: Every 50 requests per thread")
    print(f"  Estimated time: {time_estimate:.1f} hours (with {NUM_THREADS} threads)")
    print()
    print(f"Data to extract:")
    print(f"  ✓ Registration Number")
    print(f"  ✓ Registered Address")
    print(f"  ✓ Operating Status")
    print(f"  ✓ Company Age")
    print(f"  ✓ Building")
    print(f"  ✓ Contact Number")
    print(f"  ✓ Website")
    print(f"  ✓ Description")
    print(f"  ✓ Primary SSIC Code")
    print(f"  ✓ Primary Industry")
    print(f"  ✓ Secondary SSIC Code")
    print(f"  ✓ Secondary Industry")
    print(f"  ✓ Company Founder/Founded Date")
    print(f"  ✓ Social Media Links (Facebook, LinkedIn, Twitter, Instagram, etc.)")
    print()
    
    input("Press ENTER to start...")
    print()
    
    try:
        # Load data
        df = pd.read_csv(INPUT_FILE, low_memory=False)
        print(f"Loaded {len(df)} companies from CSV\n")
        
        # Filter data (single-threaded)
        temp_scraper = RecordOwlComprehensiveScraper(headless=HEADLESS)
        df = temp_scraper.filter_data(df)
        temp_scraper.close()
        
        # Get subset
        df = df.reset_index(drop=True)
        df_subset = df.iloc[START_FROM:START_FROM + NUM_COMPANIES]
        
        # Split into chunks for threads
        chunk_size = len(df_subset) // NUM_THREADS
        chunks = []
        for i in range(NUM_THREADS):
            start_idx = i * chunk_size
            if i == NUM_THREADS - 1:
                chunks.append(df_subset.iloc[start_idx:])
            else:
                chunks.append(df_subset.iloc[start_idx:start_idx + chunk_size])
        
        print(f"\nSplit {len(df_subset)} companies into {NUM_THREADS} threads:")
        for i, chunk in enumerate(chunks):
            print(f"  Thread-{i+1}: {len(chunk)} companies")
        print()
        
        # Thread-safe structures
        results_lock = threading.Lock()
        progress_lock = threading.Lock()
        results_list = []
        progress_counter = [0]
        
        # Create directories
        os.makedirs("data/bronze/recordowld/checkpoint", exist_ok=True)
        
        # Start multithreading
        start_time = time.time()
        print("🚀 Starting parallel scraping...\n")
        
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = []
            for i, chunk in enumerate(chunks):
                future = executor.submit(
                    worker_thread,
                    i + 1,
                    chunk,
                    HEADLESS,
                    results_lock,
                    results_list,
                    progress_lock,
                    progress_counter
                )
                futures.append(future)
            
            # Wait for completion
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Thread error: {e}")
        
        # Save results
        results = pd.DataFrame(results_list)
        results.to_csv(OUTPUT_FILE, index=False)
        results.to_csv('data/bronze/recordowld/recordowl_final.csv', index=False)
        
        # Final summary
        elapsed = time.time() - start_time
        total = len(results)
        found_websites = results['website'].notna().sum()
        found_phones = results['contact_number'].notna().sum()
        found_addresses = results['registered_address'].notna().sum()
        found_descriptions = results['description'].notna().sum()
        found_ssic_primary = results['primary_ssic_code'].notna().sum()
        found_ssic_secondary = results['secondary_ssic_code'].notna().sum()
        
        # Count social media
        social_platforms = ['facebook', 'linkedin', 'twitter', 'instagram', 'youtube', 'tiktok', 'pinterest']
        found_social = sum(1 for r in results.to_dict('records') if any(r.get(platform) for platform in social_platforms))
        
        print("\n" + "="*70)
        print("SCRAPING COMPLETE!")
        print("="*70)
        print(f"Total processed: {total}")
        print(f"Time taken: {elapsed/60:.1f} minutes ({elapsed/3600:.2f} hours)")
        print(f"Average: {elapsed/total:.2f} seconds per company")
        print(f"Speedup: {NUM_THREADS}x faster than single-threaded")
        print(f"\nData extracted:")
        print(f"  Websites: {found_websites} ({found_websites/total*100:.1f}%)")
        print(f"  Phone numbers: {found_phones} ({found_phones/total*100:.1f}%)")
        print(f"  Addresses: {found_addresses} ({found_addresses/total*100:.1f}%)")
        print(f"  Descriptions: {found_descriptions} ({found_descriptions/total*100:.1f}%)")
        print(f"  Primary SSIC: {found_ssic_primary} ({found_ssic_primary/total*100:.1f}%)")
        print(f"  Secondary SSIC: {found_ssic_secondary} ({found_ssic_secondary/total*100:.1f}%)")
        print(f"  Social media: {found_social} ({found_social/total*100:.1f}%)")
        print(f"\nFinal output saved to: {OUTPUT_FILE}")
        print(f"Final checkpoint: data/bronze/recordowld/checkpoint/recordowl_final.csv")
        print("="*70)
        print()
        
        # Show sample
        if found_websites > 0:
            print("Sample results (first 5 with websites):")
            sample = results[results['website'].notna()].head(5)
            for _, row in sample.iterrows():
                print(f"\n  {row['company_name'][:45]}")
                print(f"    Website: {row['website']}")
                if row.get('contact_number'):
                    print(f"    Phone: {row['contact_number']}")
                if row.get('primary_industry'):
                    print(f"    Industry: {row['primary_industry'][:50]}")
            print()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopped by user (Ctrl+C)")
        if 'results_list' in locals() and len(results_list) > 0:
            partial_file = OUTPUT_FILE.replace('.csv', '_partial.csv')
            pd.DataFrame(results_list).to_csv(partial_file, index=False)
            print(f"Saved {len(results_list)} results to: {partial_file}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        if 'results_list' in locals() and len(results_list) > 0:
            partial_file = OUTPUT_FILE.replace('.csv', '_partial.csv')
            pd.DataFrame(results_list).to_csv(partial_file, index=False)
            print(f"Saved {len(results_list)} results to: {partial_file}")