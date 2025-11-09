import os
import glob
import shutil
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime

# Load .env file
load_dotenv()

ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")


def upload_to_adls(local_path, remote_path, file_system):
    """Upload a single file to ADLS Gen2"""

    service_client = DataLakeServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ACCOUNT_KEY
    )

    fs_client = service_client.get_file_system_client(file_system)
    file_client = fs_client.get_file_client(remote_path)

    with open(local_path, "rb") as f:
        file_client.upload_data(f.read(), overwrite=True)

    print(f"✅ Uploaded: {local_path} → {remote_path}")


def process_and_archive(src, file_system):
    """
    Upload all CSV files from data/{src}
    Then move them to data/{src}/archive
    """

    base_path = f"data/{file_system}/{src}"
    archive_path = f"{base_path}/archive"

    # Create archive folder if not exists
    os.makedirs(archive_path, exist_ok=True)

    # Get all CSV files
    csv_files = glob.glob(os.path.join(base_path, "*.csv"))

    if not csv_files:
        print("No CSV files found.")
        return

    for file_path in csv_files:
        file_name = os.path.basename(file_path)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # ADLS upload path
        remote_path = f"{src}/{file_name}"

        # ✅ Upload the file
        upload_to_adls(
            local_path=file_path,
            remote_path=remote_path,
            file_system=file_system
        )

        # ✅ Move file to archive
        dest_path = os.path.join(archive_path, file_name)
        shutil.move(file_path, dest_path)
        print(f"📦 Moved to archive: {dest_path}")

    print("✅ All files processed and archived successfully.")

process_and_archive(src="acra", file_system="bronze")
process_and_archive(src="companies_sg", file_system="bronze")
process_and_archive(src="recordowld", file_system="bronze")
process_and_archive(src="scrape_websites", file_system="bronze")
process_and_archive(src="stocks", file_system="bronze")

