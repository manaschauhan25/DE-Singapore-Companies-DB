"""
Companies.sg Scraping Scheduler
"""

import schedule, time, subprocess, sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
VENV_PYTHON = BASE_DIR.parent / ".venv" / "Scripts" / "python.exe"

SCHEDULE_TYPE = "daily"  # Options: "daily", "hours", "minutes", "weekdays", "disabled"
TIME = "05:00"
INTERVAL = 6

def run_scripts():
    print(f"\n{'='*60}\n🏛️ STARTING COMPANIES.SG SCRAPING\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}\n")
    python_exe = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable
    result = subprocess.run([python_exe, str(BASE_DIR / "companies_sg" / "1_sg_scraper.py")], cwd=BASE_DIR / "companies_sg")
    print(f"{'✅' if result.returncode == 0 else '❌'} Companies.sg Scraper\n{'='*60}\n")

print("="*60 + "\n🏛️ COMPANIES.SG SCHEDULER STARTED\n" + "="*60)
if SCHEDULE_TYPE == "daily": schedule.every().day.at(TIME).do(run_scripts); print(f"📅 Daily at {TIME}")
elif SCHEDULE_TYPE == "hours": schedule.every(INTERVAL).hours.do(run_scripts); print(f"📅 Every {INTERVAL} hours")
elif SCHEDULE_TYPE == "minutes": schedule.every(INTERVAL).minutes.do(run_scripts); print(f"📅 Every {INTERVAL} minutes")
elif SCHEDULE_TYPE == "weekdays": [getattr(schedule.every(), d).at(TIME).do(run_scripts) for d in ["monday","tuesday","wednesday","thursday","friday"]]; print(f"📅 Weekdays at {TIME}")
elif SCHEDULE_TYPE == "disabled": print("⚠️ DISABLED"); sys.exit(0)
print("💡 Edit lines 12-14 to change schedule\n⏳ Waiting...\n" + "="*60)
try:
    while True: schedule.run_pending(); time.sleep(60)
except KeyboardInterrupt: print("\n🛑 Stopped")
