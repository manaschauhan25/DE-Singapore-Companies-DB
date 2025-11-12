"""
RecordOwl Scraping Scheduler
Run this script to schedule RecordOwl scraping independently
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

SCHEDULE_TYPE = "hours"  # Options: "daily", "hours", "minutes", "weekdays", "disabled"
TIME = "03:00"           # For daily/weekdays (24-hour format HH:MM)
INTERVAL = 6             # For hours/minutes schedule

# ============================================

def run_recordowl_scripts():
    """Run RecordOwl scripts"""
    print(f"\n{'='*60}")
    print(f"🦉 STARTING RECORDOWL SCRAPING")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    python_exe = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable
    script_path = BASE_DIR / "record0wld" / "1_main_record_freeze.py"
    
    print(f"▶️  Running: RecordOwl Scraper")
    result = subprocess.run(
        [python_exe, str(script_path)],
        cwd=script_path.parent
    )
    
    if result.returncode == 0:
        print(f"✅ Success: RecordOwl Scraper\n")
    else:
        print(f"❌ Failed: RecordOwl Scraper\n")
    
    print(f"{'='*60}")
    print(f"🦉 RECORDOWL SCRAPING COMPLETED")
    print(f"{'='*60}\n")


# Apply schedule
print("="*60)
print("🦉 RECORDOWL SCHEDULER STARTED")
print("="*60)

if SCHEDULE_TYPE == "daily":
    schedule.every().day.at(TIME).do(run_recordowl_scripts)
    print(f"📅 Schedule: Daily at {TIME}")
elif SCHEDULE_TYPE == "hours":
    schedule.every(INTERVAL).hours.do(run_recordowl_scripts)
    print(f"📅 Schedule: Every {INTERVAL} hours")
elif SCHEDULE_TYPE == "minutes":
    schedule.every(INTERVAL).minutes.do(run_recordowl_scripts)
    print(f"📅 Schedule: Every {INTERVAL} minutes")
elif SCHEDULE_TYPE == "weekdays":
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        getattr(schedule.every(), day).at(TIME).do(run_recordowl_scripts)
    print(f"📅 Schedule: Weekdays at {TIME}")
elif SCHEDULE_TYPE == "disabled":
    print("⚠️  Scheduler is DISABLED")
    sys.exit(0)

print("\n💡 To change schedule: Edit lines 21-23 in this file")
print("⏳ Waiting for scheduled time...\n")
print("="*60)

try:
    while True:
        schedule.run_pending()
        time.sleep(60)
except KeyboardInterrupt:
    print("\n🛑 RecordOwl Scheduler stopped")
