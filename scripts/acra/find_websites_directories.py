"""
Website Finder - FAST Version
Uses domain guessing + bulk verification
"""

import requests
import pandas as pd
from pathlib import Path
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configuration
INPUT_FILE = Path("data/bronze/acra_filtered_100k.csv")
OUTPUT_FILE = Path("data/silver/companies_with_websites.csv")
OUTPUT_DIR = Path("data/silver")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}


def clean_company_name(name):
    """Clean company name for domain creation"""
    if pd.isna(name):
        return ""
    
    name = str(name).strip()
    # Remove common suffixes
    name = re.sub(r'\s+(PTE\.?|LTD\.?|LIMITED|PRIVATE|LLP|SINGAPORE)$', '', name, flags=re.IGNORECASE)
    # Remove special characters
    name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
    # Remove extra spaces and lowercase
    name = re.sub(r'\s+', '', name.lower())
    return name


def generate_domain_variations(company_name):
    """Generate likely domain variations for a company"""
    clean_name = clean_company_name(company_name)
    
    if len(clean_name) < 3:
        return []
    
    # Common Singapore domain patterns
    domains = [
        f"{clean_name}.com.sg",
        f"{clean_name}.sg",
        f"{clean_name}.com",
    ]
    
    # Add www variants
    domains.extend([f"www.{d}" for d in domains.copy()])
    
    return domains


def check_domain_exists(domain, timeout=3):
    """Quick check if domain is reachable"""
    for protocol in ['https://', 'http://']:
        try:
            url = protocol + domain
            response = requests.head(url, timeout=timeout, allow_redirects=True, headers=HEADERS)
            
            if response.status_code < 400:
                # Found working URL
                final_url = response.url
                return final_url
                
        except:
            continue
    
    return None


def process_company(row):
    """Process a single company to find its website"""
    uen = row['uen']
    company_name = row['entity_name']
    
    # Generate domain variations
    domains = generate_domain_variations(company_name)
    
    # Try each domain
    for domain in domains:
        website = check_domain_exists(domain)
        if website:
            return {
                'uen': uen,
                'entity_name': company_name,
                'website': website,
                'source': 'domain_guess'
            }
    
    return None


def process_batch_parallel(df_batch, max_workers=20):
    """Process a batch of companies in parallel"""
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_company, row): idx 
                  for idx, row in df_batch.iterrows()}
        
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
            
            completed += 1
            if completed % 100 == 0:
                print(f"  Processed: {completed}/{len(df_batch)} | Found: {len(results)}")
    
    return pd.DataFrame(results) if results else pd.DataFrame()


def main():
    print("="*70)
    print("WEBSITE FINDER - FAST VERSION")
    print("="*70)
    print()
    
    # Load companies
    print("Loading companies...")
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df):,} companies")
    
    print("\nThis script will:")
    print("- Generate likely domain names (e.g., companyname.com.sg)")
    print("- Verify if domains exist (parallel checking)")
    print("- Process ~1000 companies per minute")
    print()
    
    # Ask how many to process
    default_count = min(30000, len(df))
    count = input(f"How many companies to process? (default {default_count}): ").strip()
    count = int(count) if count else default_count
    
    df_subset = df.head(count)
    
    # Process in batches
    batch_size = 5000
    all_results = []
    
    print(f"\nProcessing {len(df_subset):,} companies in batches of {batch_size}...")
    print("="*70)
    
    for i in range(0, len(df_subset), batch_size):
        batch_num = i // batch_size + 1
        batch = df_subset.iloc[i:i+batch_size]
        
        print(f"\nBatch {batch_num}: Processing companies {i:,} to {i+len(batch):,}")
        start_time = time.time()
        
        batch_results = process_batch_parallel(batch, max_workers=20)
        
        elapsed = time.time() - start_time
        print(f"Batch completed in {elapsed:.1f}s | Found {len(batch_results)} websites")
        
        if not batch_results.empty:
            all_results.append(batch_results)
            
            # Save progress
            temp_df = pd.concat(all_results, ignore_index=True)
            temp_df.to_csv(OUTPUT_FILE, index=False)
            print(f"Progress saved: {len(temp_df):,} total websites found")
    
    # Final results
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['uen'])
        final_df.to_csv(OUTPUT_FILE, index=False)
        
        print("\n" + "="*70)
        print("RESULTS")
        print("="*70)
        print(f"Companies processed: {len(df_subset):,}")
        print(f"Websites found: {len(final_df):,}")
        print(f"Success rate: {len(final_df)/len(df_subset)*100:.1f}%")
        print(f"Output saved: {OUTPUT_FILE}")
        print("="*70)
        
        # Show samples
        print("\nSample results:")
        for idx, row in final_df.head(10).iterrows():
            print(f"  {row['entity_name'][:40]:40s} -> {row['website']}")
        
        print(f"\n✓ Done! Found {len(final_df):,} websites")
        print(f"Next: Use Bing API for remaining {len(df_subset) - len(final_df):,} companies")
        
        return final_df
    else:
        print("\nNo websites found!")
        return None


if __name__ == "__main__":
    main()