"""
Production Selenium Scraper - FULL VERSION
- Downloads HTML (saves to Bronze)
- Extracts all available data
- Runs on all 1000 companies
- Headless=False (visible browser helps bypass CAPTCHA)
- Checkpoint saves every 100 companies
"""

import pandas as pd
import time
import os
import re
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json

class ProductionSeleniumScraper:
    def __init__(self, 
                 html_dir='data/bronze/scrape_websites/html',
                 failed_dir='data/bronze/scrape_websites/website_not_working',
                 checkpoint_dir='data/bronze/scrape_websites/checkpoint'):
        self.driver = None
        self.wait = None
        self.html_dir = html_dir
        self.failed_dir = failed_dir
        self.checkpoint_dir = checkpoint_dir
        
        # Create directories
        Path(self.html_dir).mkdir(parents=True, exist_ok=True)
        Path(self.failed_dir).mkdir(parents=True, exist_ok=True)
        Path(self.checkpoint_dir).mkdir(parents=True, exist_ok=True)
    
    def setup_driver(self):
        """Setup Chrome - NOT headless (helps with CAPTCHA)"""
        print("Setting up Chrome (visible browser)...")
        
        chrome_options = Options()
        
        # NOT HEADLESS - visible browser helps bypass detection
        # chrome_options.add_argument('--headless')  # DISABLED
        
        # Stealth settings
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Hide automation
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        self.driver.set_page_load_timeout(30)
        self.wait = WebDriverWait(self.driver, 10)
        
        print("✅ Chrome ready (visible mode for CAPTCHA bypass)!")
        print()
    
    def close_driver(self):
        if self.driver:
            self.driver.quit()
    
    def clean_filename(self, company_name):
        clean = re.sub(r'[^\w\s-]', '', company_name)
        clean = re.sub(r'[-\s]+', '_', clean)
        return clean[:50].upper()
    
    def extract_social_media(self, page_source):
        """Extract social from page source (more reliable)"""
        social = {'linkedin': None, 'facebook': None, 'instagram': None}
        
        try:
            # LinkedIn
            linkedin_match = re.search(r'https?://(?:www\.)?linkedin\.com/company/[^\s"\'<>]+', page_source)
            if linkedin_match:
                social['linkedin'] = linkedin_match.group(0).split('?')[0]
            
            # Facebook
            fb_match = re.search(r'https?://(?:www\.)?facebook\.com/[^\s"\'<>]+', page_source)
            if fb_match:
                url = fb_match.group(0).split('?')[0]
                if 'sharer' not in url and 'plugins' not in url:
                    social['facebook'] = url
            
            # Instagram
            ig_match = re.search(r'https?://(?:www\.)?instagram\.com/[^\s"\'<>]+', page_source)
            if ig_match:
                url = ig_match.group(0).split('?')[0]
                if '/p/' not in url:
                    social['instagram'] = url
        except:
            pass
        
        return social
    
    def extract_contact_info(self, page_source):
        """Extract email and phone from page source"""
        contact = {'contact_email': None, 'contact_phone': None}
        
        try:
            # Email
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            emails = re.findall(email_pattern, page_source)
            
            if emails:
                skip_terms = ['example', 'test', 'noreply', 'sentry', 'schema.org']
                valid_emails = [e for e in emails if not any(term in e.lower() for term in skip_terms)]
                
                # Prioritize contact emails
                priority = [e for e in valid_emails if any(term in e.lower() for term in ['info@', 'contact@', 'hello@', 'enquiry@'])]
                contact['contact_email'] = priority[0] if priority else (valid_emails[0] if valid_emails else None)
            
            # Phone (Singapore formats)
            phone_patterns = [
                r'\+65[-\s]?\d{4}[-\s]?\d{4}',
                r'\(65\)[-\s]?\d{4}[-\s]?\d{4}',
                r'65[-\s]\d{4}[-\s]?\d{4}',
                r'\d{4}[-\s]\d{4}'
            ]
            
            for pattern in phone_patterns:
                phones = re.findall(pattern, page_source)
                if phones:
                    contact['contact_phone'] = phones[0].strip()
                    break
        except:
            pass
        
        return contact
    
    def extract_keywords(self, page_source):
        """Extract keywords from meta tags"""
        keywords = None
        
        try:
            # Meta keywords
            meta_kw = re.search(r'<meta[^>]+name=["\']keywords["\'][^>]+content=["\'](.*?)["\']', page_source, re.I)
            if meta_kw:
                keywords = meta_kw.group(1).strip()[:300]
                return keywords
            
            # Meta description
            meta_desc = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']', page_source, re.I)
            if meta_desc:
                keywords = meta_desc.group(1).strip()[:300]
                return keywords
            
            # OG description
            og_desc = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']', page_source, re.I)
            if og_desc:
                keywords = og_desc.group(1).strip()[:300]
        except:
            pass
        
        return keywords
    
    def scrape_and_extract(self, uen, company_name, url):
        """Download HTML and extract data"""
        result = {
            'uen': uen,
            'company_name': company_name,
            'website': url,
            'linkedin': None,
            'facebook': None,
            'instagram': None,
            'contact_email': None,
            'contact_phone': None,
            'keywords': None,
            'scrape_status': 'failed',
            'html_saved': False,
            'html_size': 0,
            'error': None
        }
        
        if not url or pd.isna(url) or url.strip() == '':
            result['error'] = 'No URL'
            return result
        
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            result['website'] = url
        
        try:
            # Navigate
            self.driver.get(url)
            
            # Wait
            try:
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            except:
                pass
            
            time.sleep(3)
            
            # Scroll
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(1)
            except:
                pass
            
            # Get HTML
            html_content = self.driver.page_source
            
            # Save HTML
            try:
                clean_name = self.clean_filename(company_name)
                filename = f"{uen}_{clean_name}.html"
                filepath = os.path.join(self.html_dir, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                
                result['html_saved'] = True
                result['html_size'] = len(html_content)
            except:
                pass
            
            # Extract data
            social = self.extract_social_media(html_content)
            contact = self.extract_contact_info(html_content)
            keywords = self.extract_keywords(html_content)
            
            result.update(social)
            result.update(contact)
            result['keywords'] = keywords
            result['scrape_status'] = 'success'
            
        except Exception as e:
            error_msg = str(e).lower()
            if 'timeout' in error_msg:
                result['error'] = 'Timeout'
            else:
                result['error'] = str(e)[:100]
        
        return result
    
    def run_full_scrape(self, input_file, output_file, start_from=0, max_companies=1000):
        """Run full scrape with checkpoints"""
        print("="*70)
        print("PRODUCTION SELENIUM SCRAPER - FULL RUN")
        print("="*70)
        print()
        print("Features:")
        print("  ✓ Visible browser (helps with CAPTCHA)")
        print("  ✓ Saves HTML to Bronze layer")
        print("  ✓ Extracts all available data")
        print("  ✓ Checkpoint saves every 100 companies")
        print("  ✓ Resume capability")
        print()
        
        # Load data
        print(f"Loading: {input_file}")
        df = pd.read_csv(input_file)
        
        # Filter with websites
        df_with_websites = df[df['website'].notna() & (df['website'] != '')]
        
        # Slice for processing
        df_to_process = df_with_websites.iloc[start_from:start_from + max_companies]
        
        print(f"Total companies in file: {len(df)}")
        print(f"Companies with websites: {len(df_with_websites)}")
        print(f"Processing: {len(df_to_process)} companies")
        print(f"Starting from: {start_from}")
        print()
        
        input("Press ENTER to start (browser will open)...")
        print()
        
        # Setup browser
        self.setup_driver()
        
        results = []
        failed_sites = []
        start_time = time.time()
        
        for idx, (_, row) in enumerate(df_to_process.iterrows(), 1):
            actual_idx = start_from + idx
            uen = row['uen']
            company_name = row['company_name']
            website = row['website']
            
            print(f"[{idx}/{len(df_to_process)}] {company_name}")
            print(f"  URL: {website}")
            
            scrape_start = time.time()
            
            # Scrape and extract
            result = self.scrape_and_extract(uen, company_name, website)
            
            scrape_time = time.time() - scrape_start
            result['scrape_time'] = round(scrape_time, 1)
            
            results.append(result)
            
            # Track failures
            if result['scrape_status'] == 'failed':
                failed_sites.append({
                    'uen': uen,
                    'company_name': company_name,
                    'website': website,
                    'error': result['error']
                })
            
            # Print
            if result['scrape_status'] == 'success':
                found = []
                if result['linkedin']: found.append('LI')
                if result['facebook']: found.append('FB')
                if result['instagram']: found.append('IG')
                if result['contact_email']: found.append('Email')
                if result['contact_phone']: found.append('Phone')
                if result['keywords']: found.append('KW')
                if result['html_saved']: found.append('HTML')
                
                print(f"  ✅ {', '.join(found) if found else 'HTML only'} ({scrape_time:.1f}s)")
            else:
                print(f"  ❌ {result['error']} ({scrape_time:.1f}s)")
            
            # Progress every 10
            if idx % 10 == 0:
                elapsed = time.time() - start_time
                avg_time = elapsed / idx
                remaining = (len(df_to_process) - idx) * avg_time
                
                success = len([r for r in results if r['scrape_status'] == 'success'])
                
                print()
                print(f"  Progress: {idx}/{len(df_to_process)} ({idx/len(df_to_process)*100:.1f}%)")
                print(f"  Success: {success}/{idx} ({success/idx*100:.1f}%)")
                print(f"  Time: {elapsed/60:.1f}m | Remaining: {remaining/60:.0f}m")
                print()
            
            # Checkpoint every 100
            if idx % 100 == 0:
                checkpoint_file = os.path.join(self.checkpoint_dir, f'checkpoint_{actual_idx}.csv')
                pd.DataFrame(results).to_csv(checkpoint_file, index=False)
                print(f"  💾 Checkpoint saved: {actual_idx} companies")
                print()
            
            time.sleep(1)
        
        # Close browser
        self.close_driver()
        
        # Save final results
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_file, index=False)
        
        # Save failed
        if failed_sites:
            pd.DataFrame(failed_sites).to_csv(
                os.path.join(self.failed_dir, 'failed_scrape.csv'), 
                index=False
            )
        
        # Summary
        total_time = time.time() - start_time
        success_count = len([r for r in results if r['scrape_status'] == 'success'])
        
        print()
        print("="*70)
        print("SCRAPING COMPLETE!")
        print("="*70)
        print()
        print(f"Results saved: {output_file}")
        print()
        print("SUMMARY:")
        print(f"  Total processed: {len(results)}")
        print(f"  ✅ Success: {success_count} ({success_count/len(results)*100:.1f}%)")
        print(f"  ❌ Failed: {len(results) - success_count}")
        print()
        print("DATA EXTRACTED:")
        print(f"  LinkedIn: {sum(1 for r in results if r['linkedin'])}")
        print(f"  Facebook: {sum(1 for r in results if r['facebook'])}")
        print(f"  Instagram: {sum(1 for r in results if r['instagram'])}")
        print(f"  Emails: {sum(1 for r in results if r['contact_email'])}")
        print(f"  Phones: {sum(1 for r in results if r['contact_phone'])}")
        print(f"  Keywords: {sum(1 for r in results if r['keywords'])}")
        print(f"  HTML saved: {sum(1 for r in results if r['html_saved'])}")
        print()
        print(f"TIME: {total_time/60:.1f} minutes")
        print(f"HTML Location: {self.html_dir}/")
        print()


if __name__ == "__main__":
    INPUT_FILE = 'data/bronze/recordowld/recordowl_website_1000.csv'
    OUTPUT_FILE = 'data/bronze/scraped_websites.csv'
    os.makedirs('data/bronze/scrape_websites', exist_ok=True)
    # Configuration
    START_FROM = 0  # Set to checkpoint number to resume
    MAX_COMPANIES = 1000  # Process all
    
    scraper = ProductionSeleniumScraper()
    scraper.run_full_scrape(INPUT_FILE, OUTPUT_FILE, START_FROM, MAX_COMPANIES)
    
    print("✅ DONE! Check the output CSV for your dataset.")