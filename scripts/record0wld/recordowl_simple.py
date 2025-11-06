"""
RecordOwl Scraper - Single Worker (Clean & Simple)
Progress every 10 records
Auto-saves every 500 companies
"""

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time
import random
import os

class RecordOwlScraper:
    def __init__(self, headless=False):
        print("Setting up browser...\n")
        
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Change this to match YOUR Chrome version
        self.driver = uc.Chrome(version_main=141, options=options)
        
        print("✅ Browser ready!\n")

    def scrape_company(self, uen, company_name, retry=0):
        """Scrape single company with retry logic"""
        max_retries = 2
        
        try:
            # Search
            search_url = f"https://recordowl.com/search?name={uen}"
            
            try:
                self.driver.set_page_load_timeout(30)  # 30 second timeout
                self.driver.get(search_url)
            except Exception as e:
                if retry < max_retries:
                    print(f"    ⚠️ Timeout, retrying... ({retry + 1}/{max_retries})")
                    time.sleep(3)
                    return self.scrape_company(uen, company_name, retry + 1)
                else:
                    print(f"    ❌ Timeout after {max_retries} retries")
                    return None, None
            
            # Find company link
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/company/']"))
                )
                company_url = element.get_attribute("href")
            except:
                return None, None
            
            # Visit company page
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
                    return company_url, None
            
            time.sleep(random.uniform(1, 2))
            
            # Find website
            website = None
            try:
                links = self.driver.find_elements(By.TAG_NAME, "a")
                for link in links:
                    href = link.get_attribute('href')
                    if href and 'http' in href:
                        skip = ['recordowl.com', 'javascript', 'facebook', 'linkedin', 
                               'twitter', 'instagram', 'youtube']
                        if not any(s in href.lower() for s in skip):
                            if any(d in href.lower() for d in ['.com', '.sg', '.net', '.org']):
                                website = href
                                break
            except:
                pass
            
            # Save HTML if no website
            if not website:
                try:
                    # os.makedirs("no_website_html", exist_ok=True)
                    with open(f"data/bronze/recordowld/no_website_html/{uen}.html", "w", encoding="utf-8") as f:
                        f.write(self.driver.page_source)
                except:
                    pass
            
            # Short delay
            time.sleep(random.uniform(1, 2))
            
            return company_url, website
            
        except Exception as e:
            if "timeout" in str(e).lower() and retry < max_retries:
                print(f"    ⚠️ Timeout, retrying... ({retry + 1}/{max_retries})")
                time.sleep(3)
                return self.scrape_company(uen, company_name, retry + 1)
            else:
                print(f"    ❌ Error: {str(e)[:50]}")
                return None, None
    
    def process_batch(self, df, num_companies=10000, start_from=0):
        """Process companies with progress every 10 records"""
        print(f"Processing {num_companies} companies starting from {start_from}")
        print(f"Progress updates: Every 10 records")
        print(f"Auto-save checkpoints: Every 500 records")
        print("="*70 + "\n")
        
        results = []
        df_subset = df.iloc[start_from:start_from + num_companies]
        
        start_time = time.time()
        
        for idx, row in df_subset.iterrows():
            uen = row['uen']
            company_name = row['entity_name']
            record_num = len(results) + 1
            
            # Print current record
            print(f"[{record_num}/{num_companies}] {company_name[:50]} - {uen}")
            
            # Scrape
            company_url, website = self.scrape_company(uen, company_name)
            
            # Show result
            if website:
                print(f"    ✅ Website: {website}")
            else:
                print(f"    ❌ No website found")
            
            # Save result
            results.append({
                'uen': uen,
                'company_name': company_name,
                'company_link': company_url,
                'website': website
            })
            
            # Progress summary every 10 records
            if record_num % 10 == 0:
                elapsed = time.time() - start_time
                websites_found = sum(1 for r in results if r['website'])
                success_rate = (websites_found / record_num * 100)
                avg_time = elapsed / record_num
                remaining = (num_companies - record_num) * avg_time / 60
                
                print(f"\n{'='*70}")
                print(f"PROGRESS UPDATE - Completed {record_num}/{num_companies}")
                print(f"{'='*70}")
                print(f"Websites found: {websites_found} ({success_rate:.1f}%)")
                print(f"Time elapsed: {elapsed/60:.1f} minutes")
                print(f"Average time: {avg_time:.1f} seconds per company")
                print(f"Estimated remaining: {remaining:.0f} minutes")
                print(f"{'='*70}\n")
            
            # Checkpoint save every 500 records
            if record_num % 500 == 0:
                checkpoint_file = f'data/bronze/recordowld/checkpoint_{start_from + record_num}.csv'
                pd.DataFrame(results).to_csv(checkpoint_file, index=False)
                print(f"\n💾 CHECKPOINT SAVED: {checkpoint_file}\n")
        
        # Final save
        final_df = pd.DataFrame(results)
        final_df.to_csv('data/bronze/recordowld/recordowl_final.csv', index=False)
        print(f"\n💾 FINAL CHECKPOINT SAVED: data/bronze/recordowld/recordowl_final.csv\n")
        
        return final_df
    
    def close(self):
        try:
            self.driver.quit()
            print("\n✅ Browser closed")
        except:
            pass


# Main execution
if __name__ == "__main__":
    print("="*70)
    print("RecordOwl Scraper - Single Worker")
    print("="*70)
    print()
    
    # Configuration
    INPUT_FILE = 'data/bronze/acra_filtered_100k.csv'
    OUTPUT_FILE = 'data/bronze/recordowld/websites_recordowl_1k_fast.csv'
    NUM_COMPANIES = 1000    # Change this as needed
    START_FROM = 0           # Resume from here if needed
    HEADLESS = False         # False = see browser (recommended)
    
    # Time estimate
    time_estimate = NUM_COMPANIES * 2.5 / 3600  # 2.5 seconds per company
    print(f"Configuration:")
    print(f"  Companies: {NUM_COMPANIES}")
    print(f"  Start from: {START_FROM}")
    print(f"  Headless: {HEADLESS}")
    print(f"  Estimated time: {time_estimate:.1f} hours")
    print()
    
    input("Press ENTER to start...")
    print()
    
    scraper = None
    
    try:
        # Load data
        df = pd.read_csv(INPUT_FILE)
        print(f"Loaded {len(df)} companies from CSV\n")
        
        # Initialize scraper
        scraper = RecordOwlScraper(headless=HEADLESS)
        
        # Process companies
        results = scraper.process_batch(df, NUM_COMPANIES, START_FROM)
        
        # Save final output
        results.to_csv(OUTPUT_FILE, index=False)
        
        # Final summary
        total = len(results)
        found = results['website'].notna().sum()
        found_links = results['company_link'].notna().sum()
        
        print("\n" + "="*70)
        print("SCRAPING COMPLETE!")
        print("="*70)
        print(f"Total processed: {total}")
        print(f"Company links found: {found_links} ({found_links/total*100:.1f}%)")
        print(f"Websites found: {found} ({found/total*100:.1f}%)")
        print(f"\nFinal output saved to: {OUTPUT_FILE}")
        print(f"Final checkpoint: data/checkpoint_final.csv")
        print("="*70)
        print()
        
        # Show sample
        if found > 0:
            print("Sample websites found (first 10):")
            sample = results[results['website'].notna()].head(10)
            for _, row in sample.iterrows():
                print(f"  {row['company_name'][:45]:45} → {row['website']}")
            print()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopped by user (Ctrl+C)")
        if 'results' in locals() and len(results) > 0:
            partial_file = OUTPUT_FILE.replace('.csv', '_partial.csv')
            pd.DataFrame(results).to_csv(partial_file, index=False)
            print(f"Saved {len(results)} results to: {partial_file}")
            print("Check data/checkpoint_*.csv for saved checkpoints")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        if 'results' in locals() and len(results) > 0:
            partial_file = OUTPUT_FILE.replace('.csv', '_partial.csv')
            pd.DataFrame(results).to_csv(partial_file, index=False)
            print(f"Saved {len(results)} results to: {partial_file}")
        
    finally:
        if scraper:
            scraper.close()