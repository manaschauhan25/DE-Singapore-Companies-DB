"""
Simple Pipeline Scheduler
Runs all data extraction scripts in sequence
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Base directory
BASE_DIR = Path(__file__).parent

# Define scripts to run in order
SCRIPTS = [
    # Step 1: ACRA scraping
    BASE_DIR / "acra" / "1_scrape_acra_gov_page.py",
    BASE_DIR / "acra" / "2_get_acra_urls.py",
    BASE_DIR / "acra" / "3_extract_acra.py",
    
    # Step 2: RecordOwl scraping
    BASE_DIR / "record0wld" / "1_main_record_freeze.py",
    
    # Step 3: Companies.sg scraping
    BASE_DIR / "companies_sg" / "1_sg_scraper.py",
    
    # Step 4: Stock data
    BASE_DIR / "stocks" / "1_stock_scrape.py",
    BASE_DIR / "stocks" / "2_extract_stocks.py",
    
    # Step 5: Website scraping
    BASE_DIR / "scrape_websites" / "1_website_scraper.py",
    
    # Step 6: Merge CSV files
    BASE_DIR / "merge_csv.py",
    
    # Step 7: Upload to ADLS
    BASE_DIR / "upload_adls.py",
]


def run_script(script_path):
    """Run a single Python script"""
    if not script_path.exists():
        print(f"Script not found: {script_path}")
        return False
    
    print(f"\n{'='*60}")
    print(f"Running: {script_path.name}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=script_path.parent
        )
        
        if result.returncode == 0:
            print(f"✅ Success: {script_path.name}")
            return True
        else:
            print(f"❌ Failed: {script_path.name}")
            return False
            
    except Exception as e:
        print(f"❌ Error running {script_path.name}: {e}")
        return False


def run_pipeline():
    """Run all scripts in sequence"""
    print("\n" + "="*60)
    print("🚀 STARTING PIPELINE")
    print(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    success_count = 0
    failed_count = 0
    
    for script in SCRIPTS:
        if run_script(script):
            success_count += 1
        else:
            failed_count += 1
            # Ask user if they want to continue
            response = input(f"\n⚠️  Script failed. Continue? (y/n): ")
            if response.lower() != 'y':
                print("\n🛑 Pipeline stopped by user")
                break
    
    # Summary
    print("\n" + "="*60)
    print("✨ PIPELINE COMPLETED")
    print(f"⏰ End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✅ Success: {success_count}")
    print(f"❌ Failed: {failed_count}")
    print("="*60 + "\n")


if __name__ == "__main__":
    run_pipeline()
