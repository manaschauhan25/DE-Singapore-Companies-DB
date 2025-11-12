"""
SGX Stock Listing Scraper - For Local Execution
Run this on your local machine where network access is available

Requirements:
    pip install selenium
    - Chrome browser installed
    - ChromeDriver will be auto-downloaded by Selenium
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os

def setup_driver(headless=False):
    """Setup Chrome driver with options"""
    chrome_options = Options()
    
    if headless:
        chrome_options.add_argument('--headless')
    
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Disable automation flags
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=chrome_options)
    
    # Additional stealth
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def save_html(html_content, filename="sgx_stocks.html"):
    """Save HTML content to file"""
    with open(f'data/bronze/stocks/html/{filename}', 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"✓ Saved HTML to: {filename}")
    print(f"✓ File size: {len(html_content):,} characters")
    return filename

def get_sgx_stocks_page(url, wait_time=7):
    """
    Fetch SGX stocks page with Selenium
    
    Args:
        url: SGX page URL
        wait_time: Seconds to wait for dynamic content
    """
    driver = setup_driver(headless=False)  # Set True to run in background
    
    try:
        print(f"\n{'='*60}")
        print(f"Accessing: {url}")
        print(f"{'='*60}\n")
        
        driver.get(url)
        print("✓ Page loaded")
        
        # Wait for body
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Additional wait for JavaScript to load
        print(f"⏳ Waiting {wait_time}s for dynamic content...")
        time.sleep(wait_time)
        
        # Scroll to load lazy content
        print("⏳ Scrolling page to trigger lazy loading...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)
        
        # Get HTML
        html_content = driver.page_source
        
        print(f"\n✓ Page HTML retrieved: {len(html_content):,} characters")
        
        # Quick analysis
        print("\n" + "="*60)
        print("QUICK HTML ANALYSIS")
        print("="*60)
        
        # Count key elements
        table_count = html_content.count('<table')
        tbody_count = html_content.count('<tbody')
        tr_count = html_content.count('<tr')
        script_count = html_content.count('<script')
        
        print(f"Tables (<table>): {table_count}")
        print(f"Table bodies (<tbody>): {tbody_count}")
        print(f"Table rows (<tr>): {tr_count}")
        print(f"Scripts (<script>): {script_count}")
        
        # Check for common stock listing patterns
        if 'symbol' in html_content.lower() or 'ticker' in html_content.lower():
            print("✓ Found stock symbols/ticker references")
        if 'company' in html_content.lower() and 'name' in html_content.lower():
            print("✓ Found company name references")
        if 'price' in html_content.lower():
            print("✓ Found price references")
        
        # Save HTML
        filename = save_html(html_content)
        
        # Show first 5000 chars
        print("\n" + "="*60)
        print("FIRST 5000 CHARACTERS OF HTML")
        print("="*60)
        print(html_content[:5000])
        print("\n[... truncated ...]")
        
        return html_content, filename
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None, None
        
    finally:
        print("\n⏳ Closing browser...")
        driver.quit()
        print("✓ Browser closed")

def main():
    """Main execution"""
    
    # Correct SGX stock listing URL
    url = "https://stockanalysis.com/list/singapore-exchange/"
    
    print("\n" + "="*60)
    print("SGX STOCK LISTING SCRAPER")
    print("="*60)
    print(f"\nScraping from: {url}\n")
    
    print(f"\n{'#'*60}")
    print(f"# Fetching Singapore Exchange Stock List")
    print(f"{'#'*60}")
    
    html, filename = get_sgx_stocks_page(url, wait_time=7)
    
    if html and len(html) > 10000:  # Reasonable page size
        print(f"\n{'='*60}")
        print("SUCCESS!")
        print(f"{'='*60}")
        print(f"✓ URL: {url}")
        print(f"✓ File: {filename}")
        print(f"\nNext steps:")
        print("1. Open the HTML file in a browser to verify")
        print("2. Share the HTML file or describe the structure")
        print("3. We'll build the extraction logic")
        return html, filename
    else:
        print(f"\n✗ Failed to fetch content from: {url}")
        print("\nPossible issues:")
        print("- Page requires JavaScript execution")
        print("- Anti-bot protection")
        print("- Network/connection issue")
        return None, None

if __name__ == "__main__":
    html, filename = main()
    
    if html:
        print("\n\n" + "="*60)
        print("READY FOR NEXT STEP")
        print("="*60)
        print(f"\nHTML file saved as: {filename}")
        print("\nNow you can:")
        print("1. Open it in browser to see the structure")
        print("2. Share relevant sections for extraction logic")