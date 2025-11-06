# Singapore Company Data Enrichment Pipeline

A comprehensive solution to enrich ACRA company data with business information from multiple sources, targeting 10K-50K successfully enriched records.

## 📊 Analysis Results

From your 100K ACRA records, we identified **33,125 high-priority companies** across optimal segments:

- **Technology Companies**: 7,979 (SSIC 62-63) - Expected 70-80% success rate
- **Financial Services**: 12,776 (SSIC 64-66) - Expected 60-70% success rate  
- **Professional Services**: 12,370 (SSIC 69-71) - Expected 50-65% success rate
- **Total Live Companies**: 81,460 out of 100,000 (81.5%)

## 🚀 Quick Start

### 1. Run Analysis Only
```bash
python run_enrichment.py --analysis-only
```

### 2. Run Full Pipeline (10K target)
```bash
python run_enrichment.py --target 10000
```

### 3. Run Full Pipeline (50K target)
```bash
python run_enrichment.py --target 50000
```

## 🛠 Setup Instructions

### Prerequisites
- Python 3.7+
- pandas, requests, beautifulsoup4
- Your 100K ACRA data at `data/bronze/acra_filtered_100k.csv`

### API Keys Setup

1. Copy the configuration template:
```bash
cp config/api_keys_template.json config/api_keys.json
```

2. Add your API keys to `config/api_keys.json`:
```json
{
  "google_places_api_key": "YOUR_KEY_HERE",
  "clearbit_api_key": "YOUR_KEY_HERE",
  "serpapi_key": "YOUR_KEY_HERE"
}
```

## 🎯 Expected Results Summary

### Conservative Estimate (10K target)
- **Technology**: 2,400 enriched records
- **Financial**: 3,500 enriched records  
- **Professional**: 4,100 enriched records
- **Total**: ~10,000 enriched records

### Optimistic Estimate (50K target)
- Process all 33K high-priority companies
- Expected 15K-25K successful enrichments
- Additional segments (manufacturing, recent companies)
- Total: 20K-50K enriched records
