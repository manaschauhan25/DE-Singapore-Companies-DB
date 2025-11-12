# Entity Relationship Diagram - Singapore Companies DB

## Database Schema Overview

This diagram shows the relationships between tables in the Silver and Gold layers.

---

## ER Diagram (Mermaid)

```mermaid
erDiagram
    %% SILVER LAYER TABLES
    ACRA_CLEAN ||--o{ UNIFIED_COMPANIES : "provides base data"
    RECORDOWL_CLEAN ||--o{ UNIFIED_COMPANIES : "enriches with social"
    COMPANIES_SG_CLEAN ||--o{ UNIFIED_COMPANIES : "validates data"
    STOCKS_CLEAN ||--o{ UNIFIED_COMPANIES : "adds stock info"
    SCRAPED_WEBSITES_CLEAN ||--o{ UNIFIED_COMPANIES : "adds keywords"
    
    %% GOLD LAYER RELATIONSHIPS
    UNIFIED_COMPANIES ||--o| MASTER_COMPANIES : "consolidates to"
    UNIFIED_COMPANIES ||--o| LLM_ENRICHED_COMPANIES : "enriches via LLM"
    LLM_ENRICHED_COMPANIES ||--|| MASTER_COMPANIES : "merges into"
    
    %% SILVER LAYER - ACRA (Primary Source)
    ACRA_CLEAN {
        string uen PK "Unique Entity Number"
        string company_name
        string entity_type_description
        string entity_status_description
        date registration_incorporation_date
        string industry_code
        string industry_description
        string secondary_ssic_code
        string secondary_ssic_description
        string no_of_officers
        string address
        int founding_year
        string source_data
        datetime created_at
        datetime updated_at
    }
    
    %% SILVER LAYER - RecordOwl
    RECORDOWL_CLEAN {
        string uen_match PK "Matched UEN"
        string owl_company_name
        string recordowl_website
        string company_website
        string linkedin_url
        string facebook_url
        string instagram_url
        string phone_number
        string company_description
        string owl_ssic_code
        string owl_industry
        string owl_secondary_ssic
        string owl_secondary_industry
        string source_data
        datetime created_at
        datetime updated_at
    }
    
    %% SILVER LAYER - Companies.sg
    COMPANIES_SG_CLEAN {
        string uen_match PK "Matched UEN"
        string sg_company_name
        string companies_sg_website
        string sg_reg_date
        string sg_company_type
        string sg_entity_status
        string sg_website
        string source_data
        datetime created_at
        datetime updated_at
    }
    
    %% SILVER LAYER - Stocks
    STOCKS_CLEAN {
        string stock_symbol
        string stock_company_name
        float market_cap
        float revenue
        float stock_price
        float percent_change
        string source_data
        datetime created_at
        datetime updated_at
    }
    
    %% SILVER LAYER - Scraped Websites
    SCRAPED_WEBSITES_CLEAN {
        string uen_match PK "Matched UEN"
        string recordowl_website
        string scraped_company_name
        string scraped_linkedin
        string scraped_facebook
        string scraped_instagram
        string scraped_email
        string scraped_phone
        string scraped_keywords
        string source_data
        datetime created_at
        datetime updated_at
    }
    
    %% SILVER LAYER - Unified (Master Silver)
    UNIFIED_COMPANIES {
        string uen PK "Unique Entity Number"
        string company_name
        string website
        string industry
        string industry_code
        string secondary_industry
        string entity_status
        string company_type
        string address
        int founding_year
        string no_of_officers
        string linkedin
        string facebook
        string instagram
        string contact_email
        string contact_phone
        string company_description
        string keywords
        string stock_symbol
        float market_cap
        float revenue
        float stock_price
        float percent_change
        bit is_it_delisted
        string hq_country
        int no_of_locations_in_singapore
        datetime created_at
        datetime updated_at
    }
    
    %% GOLD LAYER - LLM Enriched
    LLM_ENRICHED_COMPANIES {
        string uen PK "Unique Entity Number"
        string keywords
        string llm_normalized_industry
        string company_size
        string products_offered
        string services_offered
        string error
        string source_of_data
        datetime created_at
        datetime updated_at
    }
    
    %% GOLD LAYER - Master Companies (Final)
    MASTER_COMPANIES {
        string uen PK "Unique Entity Number"
        string company_name
        string website
        string hq_country
        int no_of_locations_in_singapore
        string linkedin
        string facebook
        string instagram
        string industry
        int number_of_employees
        string company_size
        bit is_it_delisted
        string stock_exchange_code
        float revenue
        int founding_year
        string products_offered
        string services_offered
        string keywords
        datetime created_at
        datetime updated_at
    }
```

---

## Table Relationships Explained

### Silver Layer (Data Sources)

1. **acra_clean** (Primary/Base)
   - Main source of company registration data
   - Contains UEN (Unique Entity Number) - primary key
   - Provides: company names, registration dates, industry codes, addresses

2. **recordowl_clean** (Social Media & Websites)
   - Links via UEN matching
   - Provides: websites, LinkedIn, Facebook, Instagram, phone numbers

3. **companies_sg_clean** (Validation)
   - Links via UEN matching
   - Used for validating company data and finding duplicates

4. **stocks_clean** (Financial Data)
   - Matched via fuzzy matching with company names
   - Provides: stock symbols, market cap, revenue, stock prices

5. **scraped_websites_clean** (Additional Info)
   - Links via UEN matching
   - Provides: keywords, additional social media, emails

6. **unified_companies** (Master Silver Table)
   - Consolidates all 5 sources above
   - One record per UEN
   - Ready for gold layer processing

### Gold Layer (Enriched Data)

1. **llm_enriched_companies** (LLM Processing)
   - Generated from unified_companies
   - LLM extracts: normalized industry, company size, products/services
   - Links via UEN (one-to-one with unified_companies)

2. **master_companies** (Final Output)
   - Merges unified_companies + llm_enriched_companies
   - Complete company profile with all data
   - Analytics-ready for reporting

---

## Data Flow Summary

```
ACRA (100K+)  ──┐
RecordOwl     ──┤
Companies.sg  ──┼──> UNIFIED_COMPANIES ──┬──> LLM_ENRICHED ──┐
Stocks        ──┤                          │                   │
Websites      ──┘                          └──────────────────┼──> MASTER_COMPANIES
                                                               │        (GOLD)
                                                               │
                                               (Merge via UEN) ┘
```

---

## Key Relationships

| From Table | To Table | Relationship | Key |
|------------|----------|--------------|-----|
| acra_clean | unified_companies | One-to-One | uen |
| recordowl_clean | unified_companies | One-to-One | uen_match → uen |
| companies_sg_clean | unified_companies | One-to-One | uen_match → uen |
| stocks_clean | unified_companies | Many-to-One | fuzzy match on company_name |
| scraped_websites_clean | unified_companies | One-to-One | uen_match → uen |
| unified_companies | llm_enriched_companies | One-to-One | uen |
| unified_companies + llm_enriched | master_companies | One-to-One | uen |

---

## Notes

1. **Primary Key**: `uen` (Unique Entity Number) - Singapore's official business identifier
2. **Matching Strategy**: 
   - UEN matching for ACRA, RecordOwl, Companies.sg, Websites
   - Fuzzy matching for Stocks (by company name)
3. **Data Quality**: unified_companies performs deduplication and null handling
4. **CDC Tracking**: All tables have `updated_at` for incremental loading
5. **Final Output**: master_companies contains ~10K-50K enriched company records

---

## Schema Versions

- **Silver Layer**: Cleaned and normalized data
- **Gold Layer**: Business-ready enriched data
- **Version**: 1.0.0
- **Last Updated**: November 2024
