"""
ACRA Scraping Scheduler
Run this script to schedule ACRA scraping independently
"""

import schedule
import time
import subprocess
import sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"

# ============================================
# CONFIGURE SCHEDULE HERE - EDIT THIS SECTION
# ============================================

SCHEDULE_TYPE = "daily"  # Options: "daily", "hours", "minutes", "weekdays", "disabled"
TIME = "01:00"           # For daily/weekdays (24-hour format HH:MM)
INTERVAL = 6             # For hours/minutes schedule

# ============================================

def run_acra_scripts():
    """Run all ACRA scripts in sequence"""
    print(f"\n{'='*60}")
    print(f"🏢 STARTING ACRA SCRAPING")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    python_exe = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable
    
    scripts = [
        ("1_scrape_acra_gov_page.py", "Scrape ACRA Gov Page"),
        ("2_get_acra_urls.py", "Get ACRA URLs"),
        ("3_extract_acra.py", "Extract ACRA Data")
    ]
    
    for script_name, description in scripts:
        script_path = BASE_DIR / "acra" / script_name
        print(f"▶️  Running: {description}")
        
        result = subprocess.run(
            [python_exe, str(script_path)],
            cwd=script_path.parent
        )
        
        if result.returncode == 0:
            print(f"✅ Success: {description}\n")
        else:
            print(f"❌ Failed: {description}\n")
    
    print(f"{'='*60}")
    print(f"🏢 ACRA SCRAPING COMPLETED")
    print(f"{'='*60}\n")


# Apply schedule based on configuration
print("="*60)
print("🏢 ACRA SCHEDULER STARTED")
print("="*60)

if SCHEDULE_TYPE == "daily":
    schedule.every().day.at(TIME).do(run_acra_scripts)
    print(f"📅 Schedule: Daily at {TIME}")
    
elif SCHEDULE_TYPE == "hours":
    schedule.every(INTERVAL).hours.do(run_acra_scripts)
    print(f"📅 Schedule: Every {INTERVAL} hours")
    
elif SCHEDULE_TYPE == "minutes":
    schedule.every(INTERVAL).minutes.do(run_acra_scripts)
    print(f"📅 Schedule: Every {INTERVAL} minutes")
    
elif SCHEDULE_TYPE == "weekdays":
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        getattr(schedule.every(), day).at(TIME).do(run_acra_scripts)
    print(f"📅 Schedule: Weekdays at {TIME}")
    
elif SCHEDULE_TYPE == "disabled":
    print("⚠️  Scheduler is DISABLED")
    sys.exit(0)

print("\n💡 To change schedule: Edit lines 17-19 in this file")
print("⏳ Waiting for scheduled time...\n")
print("="*60)

try:
    while True:
        schedule.run_pending()
        time.sleep(60)
except KeyboardInterrupt:
    print("\n🛑 ACRA Scheduler stopped")
