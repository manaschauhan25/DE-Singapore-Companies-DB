"""
RecordOwl Scraper - PARALLEL VERSION (FIXED)
Uses multiple browser instances to scrape faster
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class ParallelRecordOwlScraper:
    def __init__(self, worker_id, headless=True):
        self.worker_id = worker_id
        print(f"Worker {worker_id}: Starting browser...")
        
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        try:
            self.driver = uc.Chrome(version_main=141, options=options)
            print(f"Worker {worker_id}: ✅ Browser ready")
        except Exception as e:
            print(f"Worker {worker_id}: ❌ Browser failed: {e}")
            raise
        
    def scrape_company(self, uen, company_name):
        """Scrape single company"""
        try:
            # Search
            search_url = f"https://recordowl.com/search?name={uen}"
            self.driver.get(search_url)
            
            # Find company link
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/company/']"))
                )
                company_url = element.get_attribute("href")
            except:
                return {
                    'uen': uen,
                    'company_name': company_name,
                    'company_link': None,
                    'website': None
                }
            
            # Visit company page
            self.driver.get(company_url)
            time.sleep(random.uniform(1, 2))
            
            # Find website
            website = None
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
            
            # Save HTML if no website
            if not website:
                try:
                    os.makedirs("no_website_html", exist_ok=True)
                    with open(f"no_website_html/{uen}.html", "w", encoding="utf-8") as f:
                        f.write(self.driver.page_source)
                except:
                    pass
            
            time.sleep(random.uniform(1, 3))
            
            return {
                'uen': uen,
                'company_name': company_name,
                'company_link': company_url,
                'website': website
            }
            
        except Exception as e:
            print(f"Worker {self.worker_id}: Error on {uen}: {e}")
            return {
                'uen': uen,
                'company_name': company_name,
                'company_link': None,
                'website': None
            }
    
    def close(self):
        try:
            self.driver.quit()
        except:
            pass


def worker_process(worker_id, companies_chunk, results_list, progress_lock, progress_counter):
    """Worker function for parallel processing"""
    scraper = None
    try:
        scraper = ParallelRecordOwlScraper(worker_id, headless=False)
        
        for idx, row in companies_chunk.iterrows():
            result = scraper.scrape_company(row['uen'], row['entity_name'])
            
            with progress_lock:
                results_list.append(result)
                progress_counter[0] += 1
                
                if progress_counter[0] % 100 == 0:
                    found = sum(1 for r in results_list if r.get('website'))
                    print(f"Worker {worker_id}: Progress {progress_counter[0]} | Found: {found}")
                    
    except Exception as e:
        print(f"Worker {worker_id}: Fatal error: {e}")
    finally:
        if scraper:
            scraper.close()


def parallel_scrape(df, num_companies=20000, num_workers=1):
    """Main parallel scraping function"""
    
    print("="*70)
    print(f"PARALLEL SCRAPING - {num_workers} Workers")
    print("="*70)
    
    # Calculate time
    time_per_company = 2.5
    total_time = (num_companies / num_workers) * time_per_company / 3600
    
    print(f"\nConfiguration:")
    print(f"  Companies: {num_companies}")
    print(f"  Workers: {num_workers}")
    print(f"  Estimated time: {total_time:.1f} hours")
    print(f"  Auto-save: Every 500 companies ✅")
    print()
    
    input("Press ENTER to start...")
    
    # Prepare data
    df_subset = df.head(num_companies)
    chunk_size = len(df_subset) // num_workers
    
    # Split into chunks
    chunks = []
    for i in range(num_workers):
        start = i * chunk_size
        end = start + chunk_size if i < num_workers - 1 else len(df_subset)
        chunks.append(df_subset.iloc[start:end])
        print(f"Worker {i}: Will process {len(df_subset.iloc[start:end])} companies")
    
    # Shared data structures
    results_list = []
    progress_lock = threading.Lock()
    progress_counter = [0]
    last_save = [0]
    
    # Start workers
    print(f"\nStarting {num_workers} browser instances...\n")
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for worker_id, chunk in enumerate(chunks):
            future = executor.submit(worker_process, worker_id, chunk, 
                                    results_list, progress_lock, progress_counter)
            futures.append(future)
        
        # Monitor progress and save checkpoints
        while any(not f.done() for f in futures):
            time.sleep(10)
            
            with progress_lock:
                current_count = progress_counter[0]
                if current_count - last_save[0] >= 500 and current_count > 0:
                    try:
                        checkpoint_file = f'data/checkpoint_{current_count}.csv'
                        pd.DataFrame(results_list).to_csv(checkpoint_file, index=False)
                        found = sum(1 for r in results_list if r.get('website'))
                        print(f"\n💾 CHECKPOINT: {checkpoint_file} ({current_count} done, {found} websites)\n")
                        last_save[0] = current_count
                    except Exception as e:
                        print(f"Checkpoint save error: {e}")
        
        # Wait for all to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Worker error: {e}")
    
    # Convert to DataFrame
    if len(results_list) > 0:
        final_df = pd.DataFrame(results_list)
        
        # Final save
        try:
            final_df.to_csv('data/checkpoint_final.csv', index=False)
            print(f"\n💾 FINAL CHECKPOINT: data/checkpoint_final.csv\n")
        except Exception as e:
            print(f"Final save error: {e}")
    else:
        print("\n⚠️ No results collected!")
        final_df = pd.DataFrame(columns=['uen', 'company_name', 'company_link', 'website'])
    
    print(f"\n{'='*70}")
    print("SCRAPING COMPLETE!")
    print(f"{'='*70}")
    
    return final_df


# Main execution
if __name__ == "__main__":
    # Configuration
    INPUT_FILE = 'data/bronze/acra_filtered_100k.csv'
    OUTPUT_FILE = 'data/silver/websites_recordowl_20k_fast.csv'
    NUM_COMPANIES = 10000    # Start with 10K for 1 hour
    NUM_WORKERS = 1
    
    print("\n⚡ FAST PARALLEL SCRAPER")
    print(f"Time: ~{(NUM_COMPANIES / NUM_WORKERS * 2.5 / 3600):.1f} hours with {NUM_WORKERS} workers")
    print()
    
    try:
        # Load data
        df = pd.read_csv(INPUT_FILE)
        print(f"Loaded {len(df)} companies\n")
        
        # Scrape in parallel
        results = parallel_scrape(df, NUM_COMPANIES, NUM_WORKERS)
        
        # Save final
        if len(results) > 0:
            results.to_csv(OUTPUT_FILE, index=False)
            
            # Summary
            total = len(results)
            found = results['website'].notna().sum() if 'website' in results.columns else 0
            
            print(f"\n{'='*70}")
            print(f"FINAL SUMMARY")
            print(f"{'='*70}")
            print(f"Total processed: {total}")
            print(f"Websites found: {found} ({found/total*100:.1f}%)")
            print(f"Saved to: {OUTPUT_FILE}")
            print(f"{'='*70}\n")
        else:
            print("\n⚠️ No results to save!")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Stopped by user")
        print("Check data/checkpoint_*.csv for partial results")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Check data/checkpoint_*.csv for partial results")