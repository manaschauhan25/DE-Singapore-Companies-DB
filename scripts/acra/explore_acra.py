"""
ACRA Data Exploration & Validation
===================================
Explore the extracted ACRA data and prepare for website discovery

Run this AFTER 01_extract_acra.py completes successfully
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json

# Load extracted data
print("=" * 70)
print("ACRA DATA EXPLORATION")
print("=" * 70)
print()

# File path
acra_file = Path("data/bronze/acra/acra_data.csv")

if not acra_file.exists():
    print(f"ERROR: File not found: {acra_file}")
    print("Please run 01_extract_acra.py first!")
    exit(1)

print(f"Loading data from: {acra_file}")
df = pd.read_csv(acra_file, low_memory=False)

print(f"✓ Loaded {len(df)} records")
print()

# ============================================================================
# 1. BASIC DATA OVERVIEW
# ============================================================================
print("1. BASIC DATA OVERVIEW")
print("-" * 70)
print(f"Total Records: {len(df):,}")
print(f"Total Columns: {len(df.columns)}")
print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
print()

# ============================================================================
# 2. COLUMN ANALYSIS
# ============================================================================
print("2. AVAILABLE COLUMNS")
print("-" * 70)
for i, col in enumerate(df.columns, 1):
    non_null = df[col].notna().sum()
    coverage = (non_null / len(df)) * 100
    print(f"{i:2d}. {col:40s} | Coverage: {coverage:5.1f}% | Non-null: {non_null:,}")
print()

# ============================================================================
# 3. KEY FIELDS VALIDATION
# ============================================================================
print("3. KEY FIELDS VALIDATION")
print("-" * 70)

# UEN (Primary Key)
uen_col = None
for col in ['uen', 'UEN', 'entity_number', 'unique_entity_number']:
    if col in df.columns:
        uen_col = col
        break

if uen_col:
    unique_uens = df[uen_col].nunique()
    print(f"✓ UEN Field: '{uen_col}'")
    print(f"  Total UENs: {unique_uens:,}")
    print(f"  Duplicates: {len(df) - unique_uens:,}")
    print(f"  Null UENs: {df[uen_col].isna().sum():,}")
else:
    print("✗ WARNING: No UEN column found!")

# Company Name
name_col = None
for col in ['entity_name', 'company_name', 'name']:
    if col in df.columns:
        name_col = col
        break

if name_col:
    print(f"✓ Name Field: '{name_col}'")
    print(f"  Null names: {df[name_col].isna().sum():,}")
else:
    print("✗ WARNING: No company name column found!")

# Industry/SSIC
ssic_col = None
for col in ['primary_ssic_code', 'ssic_code', 'industry_code']:
    if col in df.columns:
        ssic_col = col
        break

if ssic_col:
    print(f"✓ Industry Field: '{ssic_col}'")
    print(f"  Unique codes: {df[ssic_col].nunique():,}")
    print(f"  Null codes: {df[ssic_col].isna().sum():,}")
else:
    print("⚠ WARNING: No SSIC code column found")

print()

# ============================================================================
# 4. ENTITY TYPE DISTRIBUTION
# ============================================================================
print("4. ENTITY TYPE DISTRIBUTION")
print("-" * 70)

entity_type_col = None
for col in ['entity_type', 'entity_type_description', 'company_type']:
    if col in df.columns:
        entity_type_col = col
        break

if entity_type_col:
    print(df[entity_type_col].value_counts())
else:
    print("Column not found")
print()

# ============================================================================
# 5. ENTITY STATUS DISTRIBUTION
# ============================================================================
print("5. ENTITY STATUS DISTRIBUTION")
print("-" * 70)

status_col = None
for col in ['entity_status', 'status', 'company_status']:
    if col in df.columns:
        status_col = col
        break

if status_col:
    print(df[status_col].value_counts())
else:
    print("Column not found")
print()

# ============================================================================
# 6. REGISTRATION DATE ANALYSIS
# ============================================================================
print("6. REGISTRATION DATE ANALYSIS")
print("-" * 70)

date_col = None
for col in ['registration_date', 'reg_date', 'incorporation_date']:
    if col in df.columns:
        date_col = col
        break

if date_col:
    df['reg_year'] = pd.to_datetime(df[date_col], errors='coerce').dt.year
    print(f"Year Range: {df['reg_year'].min():.0f} - {df['reg_year'].max():.0f}")
    print(f"\nCompanies by Decade:")
    decade_dist = df['reg_year'].apply(lambda x: f"{int(x//10)*10}s" if pd.notna(x) else 'Unknown').value_counts().sort_index()
    for decade, count in decade_dist.head(10).items():
        print(f"  {decade}: {count:,}")
else:
    print("Column not found")
print()

# ============================================================================
# 7. TOP INDUSTRIES (SSIC CODES)
# ============================================================================
print("7. TOP 10 INDUSTRIES BY SSIC CODE")
print("-" * 70)

if ssic_col:
    ssic_desc_col = None
    for col in ['primary_ssic_description', 'ssic_description', 'industry_description']:
        if col in df.columns:
            ssic_desc_col = col
            break
    
    if ssic_desc_col:
        top_industries = df[ssic_desc_col].value_counts().head(10)
        for i, (industry, count) in enumerate(top_industries.items(), 1):
            print(f"{i:2d}. {industry[:50]:50s} : {count:,}")
    else:
        top_codes = df[ssic_col].value_counts().head(10)
        for i, (code, count) in enumerate(top_codes.items(), 1):
            print(f"{i:2d}. Code {code:10s} : {count:,}")
else:
    print("No SSIC data available")
print()

# ============================================================================
# 8. CHECK FOR WEBSITE/URL FIELDS
# ============================================================================
print("8. WEBSITE/URL FIELD CHECK")
print("-" * 70)

website_cols = [col for col in df.columns if any(term in col.lower() for term in ['website', 'url', 'web', 'http'])]

if website_cols:
    print(f"✓ Found {len(website_cols)} potential website columns:")
    for col in website_cols:
        non_null = df[col].notna().sum()
        coverage = (non_null / len(df)) * 100
        print(f"  - {col}: {coverage:.1f}% coverage ({non_null:,} records)")
        if non_null > 0:
            print(f"    Sample: {df[col].dropna().head(3).tolist()}")
else:
    print("✗ No website/URL columns found in ACRA data")
    print("→ We need to find websites from other sources!")
print()

# ============================================================================
# 9. ADDRESS DATA ANALYSIS
# ============================================================================
print("9. ADDRESS DATA COMPLETENESS")
print("-" * 70)

address_fields = ['street_name', 'postal_code', 'block', 'building_name']
for field in address_fields:
    if field in df.columns:
        coverage = (df[field].notna().sum() / len(df)) * 100
        print(f"  {field:20s}: {coverage:5.1f}% coverage")
print()

# ============================================================================
# 10. DATA QUALITY SUMMARY
# ============================================================================
print("10. DATA QUALITY SUMMARY")
print("-" * 70)

# Calculate completeness for key fields
key_fields = {
    'UEN': uen_col,
    'Company Name': name_col,
    'Industry Code': ssic_col,
    'Registration Date': date_col
}

print("Completeness for key fields:")
for field_name, col in key_fields.items():
    if col and col in df.columns:
        coverage = (df[col].notna().sum() / len(df)) * 100
        status = "✓" if coverage >= 95 else "⚠" if coverage >= 80 else "✗"
        print(f"  {status} {field_name:20s}: {coverage:5.1f}%")
print()

# ============================================================================
# 11. PREPARE FOR WEBSITE DISCOVERY
# ============================================================================
print("11. PREPARING FOR WEBSITE DISCOVERY")
print("-" * 70)

# Create a clean dataset for website discovery
website_discovery_cols = []

if uen_col:
    website_discovery_cols.append(uen_col)
if name_col:
    website_discovery_cols.append(name_col)
if ssic_col:
    website_discovery_cols.append(ssic_col)

# Add any address fields that might help
for col in ['postal_code', 'street_name']:
    if col in df.columns:
        website_discovery_cols.append(col)

if website_discovery_cols:
    df_discovery = df[website_discovery_cols].copy()
    
    # Remove duplicates based on UEN
    if uen_col:
        df_discovery = df_discovery.drop_duplicates(subset=[uen_col])
    
    # Save for next phase
    output_file = Path("data/bronze/acra_for_website_discovery.csv")
    df_discovery.to_csv(output_file, index=False)
    
    print(f"✓ Prepared dataset for website discovery")
    print(f"  File: {output_file}")
    print(f"  Records: {len(df_discovery):,}")
    print(f"  Columns: {df_discovery.columns.tolist()}")
else:
    print("✗ Cannot prepare discovery dataset - missing key columns")
print()

# ============================================================================
# 12. GENERATE JSON SUMMARY
# ============================================================================
summary = {
    "total_records": int(len(df)),
    "total_columns": int(len(df.columns)),
    "has_uen": bool(uen_col),
    "has_company_name": bool(name_col),
    "has_industry_code": bool(ssic_col),
    "has_website_field": bool(website_cols),
    "unique_companies": int(df[uen_col].nunique()) if uen_col else 0,
    "year_range": {
        "min": int(df['reg_year'].min()) if 'reg_year' in df.columns else None,
        "max": int(df['reg_year'].max()) if 'reg_year' in df.columns else None
    },
    "ready_for_website_discovery": bool(uen_col and name_col),
    "columns": df.columns.tolist()
}

summary_file = Path("data/bronze/acra/json/acra_summary.json")
with open(summary_file, 'w') as f:
    json.dump(summary, f, indent=2)

print(f"✓ Summary saved to: {summary_file}")
print()

# ============================================================================
# FINAL STATUS
# ============================================================================
print("=" * 70)
print("EXPLORATION COMPLETE")
print("=" * 70)

if uen_col and name_col:
    print("✓ Data quality: GOOD - Ready for website discovery")
    print(f"✓ Next step: Run website discovery on {len(df):,} companies")
else:
    print("⚠ Data quality: ISSUES - Check missing fields above")
    
print("=" * 70)