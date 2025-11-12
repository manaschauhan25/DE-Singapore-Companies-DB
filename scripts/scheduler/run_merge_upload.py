"""Merge CSV & Upload to ADLS Scheduler"""
import schedule, time, subprocess, sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).parent
VENV_PYTHON = BASE_DIR.parent / ".venv" / "Scripts" / "python.exe"

SCHEDULE_TYPE = "daily"  # daily, hours, minutes, weekdays, disabled
TIME = "11:00"
INTERVAL = 6

def run_scripts():
    print(f"\n{'='*60}\n📦 STARTING MERGE & UPLOAD\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}\n")
    python_exe = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable
    for script in ["merge_csv.py", "upload_adls.py"]:
        result = subprocess.run([python_exe, str(BASE_DIR / script)], cwd=BASE_DIR)
        print(f"{'✅' if result.returncode == 0 else '❌'} {script}")
    print(f"{'='*60}\n")

print("="*60 + "\n📦 MERGE & UPLOAD SCHEDULER STARTED\n" + "="*60)
if SCHEDULE_TYPE == "daily": schedule.every().day.at(TIME).do(run_scripts); print(f"📅 Daily at {TIME}")
elif SCHEDULE_TYPE == "hours": schedule.every(INTERVAL).hours.do(run_scripts); print(f"📅 Every {INTERVAL} hours")
elif SCHEDULE_TYPE == "minutes": schedule.every(INTERVAL).minutes.do(run_scripts); print(f"📅 Every {INTERVAL} minutes")
elif SCHEDULE_TYPE == "weekdays": [getattr(schedule.every(), d).at(TIME).do(run_scripts) for d in ["monday","tuesday","wednesday","thursday","friday"]]; print(f"📅 Weekdays at {TIME}")
elif SCHEDULE_TYPE == "disabled": print("⚠️ DISABLED"); sys.exit(0)
print("💡 Edit lines 9-11\n⏳ Waiting...\n" + "="*60)
try:
    while True: schedule.run_pending(); time.sleep(60)
except KeyboardInterrupt: print("\n🛑 Stopped")
