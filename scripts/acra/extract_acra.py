"""
ACRA Data Extractor
Extracts company data from Singapore government data.gov.sg
"""

import requests
import pandas as pd
from pathlib import Path
import time
import json
import glob
import os

# Configuration
OUTPUT_DIR = Path("data/bronze/acra/stage")
FINAL_OUTPUT = Path("data/bronze/acra/acra_data.csv")
API_BASE = "https://api-open.data.gov.sg/v1/public/api/datasets"
ACRA_URL_PATH = "data/bronze/acra/json/acra_dataset_ids.json"
DELETE_CSV_PATH='data/bronze//acra/stage/'
DATASET_IDS=None
# Path to your JSON file

with open(ACRA_URL_PATH, "r", encoding="utf-8") as f:
    DATASET_IDS = json.load(f)
# Dataset IDs for A-Z + Others

TARGET_RECORDS = 800000

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def delete_csv_files(directory_path):
    """
    Deletes all CSV files under the given directory.

    :param directory_path: Path where CSV files are located
    """
    csv_files = glob.glob(os.path.join(directory_path, "*.csv"))

    for file_path in csv_files:
        try:
            os.remove(file_path)
            print(f"✅ Deleted: {file_path}")
        except Exception as e:
            print(f"❌ Error deleting {file_path}: {e}")


def get_download_url(dataset_id):
    """Get S3 download URL from data.gov.sg API"""
    url = f"{API_BASE}/{dataset_id}/initiate-download"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code in [200, 201]:
            data = response.json()
            if data.get('code') == 0:
                return data['data']['url']
    except Exception as e:
        print(f"Error getting download URL: {e}")
    
    return None


def download_dataset(letter, dataset_id):
    """Download ACRA dataset for a specific letter"""
    print(f"Downloading dataset '{letter}'...")
    
    # Get download URL
    download_url = get_download_url(dataset_id)
    if not download_url:
        print(f"Failed to get download URL for {letter}")
        return None
    
    # Download CSV
    try:
        response = requests.get(download_url, timeout=300, stream=True)
        response.raise_for_status()
        
        output_file = OUTPUT_DIR / f"stage_{letter}.csv"
        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        size_mb = output_file.stat().st_size / 1024 / 1024
        print(f"Downloaded {letter}: {size_mb:.1f}MB")
        return output_file
        
    except Exception as e:
        print(f"Download failed for {letter}: {e}")
        return None


def filter_data(df):
    """Apply filters to keep only relevant companies"""
    original = len(df)
    
    # Keep only Live companies
    status_col = next((c for c in df.columns if 'status' in c.lower()), None)
    if status_col:
        df = df[df[status_col].str.contains('Live', case=False, na=False)]
    
    # Keep only Companies and LLPs
    type_col = next((c for c in df.columns if 'entity_type' in c.lower()), None)
    if type_col:
        df = df[df[type_col].str.contains('Company|Partnership', case=False, na=False)]
    
    # Keep only companies registered after 2005
    date_col = next((c for c in df.columns if 'incorporation' in c.lower()), None)
    if date_col:
        df['reg_year'] = pd.to_datetime(df[date_col], errors='coerce').dt.year
        df = df[df['reg_year'] >= 2005]
    
    # Keep only companies with industry code
    ssic_col = next((c for c in df.columns if 'ssic' in c.lower()), None)
    if ssic_col:
        df = df[df[ssic_col].notna()]
    
    print(f"Filtered: {original:,} -> {len(df):,} records")
    return df


def main():
    print("="*70)
    print("ACRA Data Extraction")
    print("="*70)
    print()
    
    all_data = []
    total = 0
    
    for letter, dataset_id in DATASET_IDS.items():
        # Download
        csv_file = download_dataset(letter, dataset_id)
        if not csv_file:
            continue
        
        # Load and filter
        try:
            df = pd.read_csv(csv_file, low_memory=False)
            
            if total == 0:
                print(f"\nColumns found: {df.columns.tolist()[:10]}...")
            
            # df = filter_data(df)
            
            if not df.empty:
                all_data.append(df)
                total += len(df)
                print(f"Total so far: {total:,} records\n")
                
                if total >= TARGET_RECORDS:
                    print(f"Target of {TARGET_RECORDS:,} reached!")
                    break
                    
        except Exception as e:
            print(f"Error processing {letter}: {e}")
        
        time.sleep(2)  # Be nice to the API
    
    if not all_data:
        print("No data extracted!")
        return
    
    # Combine and save
    print("\nCombining datasets...")
    final_df = pd.concat(all_data, ignore_index=True)
    
    if len(final_df) > TARGET_RECORDS:
        final_df = final_df.head(TARGET_RECORDS)
    
    final_df.to_csv(FINAL_OUTPUT, index=False)
    
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Records: {len(final_df):,}")
    print(f"Columns: {len(final_df.columns)}")
    print(f"Output: {FINAL_OUTPUT}")
    
    # Show top industries
    ssic_desc = next((c for c in final_df.columns if 'ssic' in c.lower() and 'description' in c.lower()), None)
    if ssic_desc:
        print(f"\nTop 5 Industries:")
        for ind, cnt in final_df[ssic_desc].value_counts().head(5).items():
            print(f"  {ind}: {cnt:,}")
    
    print("="*70)
    print("\nDone! Run 02_explore_acra.py to analyze the data.")

    delete_csv_files(DELETE_CSV_PATH)


if __name__ == "__main__":
    main()