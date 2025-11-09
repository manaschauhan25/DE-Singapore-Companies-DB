"""
Extract Stock Data from SGX HTML
Extracts symbol, company name, market cap, stock price, %change, and revenue
from the saved sgx_stocks.html file
"""

from bs4 import BeautifulSoup
import pandas as pd
import re
import os

def clean_text(text):
    """Clean text by removing extra whitespace"""
    if text:
        return ' '.join(text.strip().split())
    return ''

def extract_stocks_from_html(html_file):
    """
    Extract stock data from the HTML file
    
    Returns:
        pandas.DataFrame with columns: symbol, company_name, market_cap, 
        stock_price, percent_change, revenue
    """
    
    print(f"\n{'='*60}")
    print("EXTRACTING STOCK DATA FROM HTML")
    print(f"{'='*60}\n")
    
    # Read HTML file
    print(f"Reading: {html_file}")
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    print(f"✓ File size: {len(html_content):,} characters\n")
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the main table
    table = soup.find('table', {'id': 'main-table'})
    
    if not table:
        print("✗ Could not find main-table")
        return None
    
    print("✓ Found main stock table\n")
    
    # Extract data
    stocks_data = []
    tbody = table.find('tbody')
    
    if not tbody:
        print("✗ Could not find tbody")
        return None
    
    rows = tbody.find_all('tr')
    print(f"Found {len(rows)} table rows\n")
    
    for idx, row in enumerate(rows, 1):
        try:
            cells = row.find_all('td')
            
            if len(cells) < 7:
                continue
            
            # Extract data from each cell
            # Structure: No, Symbol, Company Name, Market Cap, Stock Price, % Change, Revenue
            
            # Symbol (cell 1)
            symbol_cell = cells[1]
            symbol_link = symbol_cell.find('a')
            symbol = clean_text(symbol_link.text) if symbol_link else clean_text(symbol_cell.text)
            
            # Company Name (cell 2)
            company_name = clean_text(cells[2].text)
            
            # Market Cap (cell 3)
            market_cap = clean_text(cells[3].text)
            
            # Stock Price (cell 4)
            stock_price = clean_text(cells[4].text)
            
            # % Change (cell 5)
            percent_change = clean_text(cells[5].text)
            
            # Revenue (cell 6)
            revenue = clean_text(cells[6].text)
            
            stocks_data.append({
                'symbol': symbol,
                'company_name': company_name,
                'market_cap': market_cap,
                'stock_price': stock_price,
                'percent_change': percent_change,
                'revenue': revenue
            })
            
            # Progress indicator
            if idx % 100 == 0:
                print(f"Processed {idx} stocks...")
                
        except Exception as e:
            print(f"✗ Error processing row {idx}: {e}")
            continue
    
    # Create DataFrame
    df = pd.DataFrame(stocks_data)
    
    print(f"\n{'='*60}")
    print("EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"✓ Total stocks extracted: {len(df)}")
    print(f"✓ Columns: {', '.join(df.columns)}")
    
    return df

def save_to_csv(df, output_file):
    """Save DataFrame to CSV"""
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\n✓ Saved to: {output_file}")
    print(f"✓ File size: {os.path.getsize(output_file):,} bytes")

def display_sample(df, n=10):
    """Display sample of the data"""
    print(f"\n{'='*60}")
    print(f"SAMPLE DATA (First {n} rows)")
    print(f"{'='*60}\n")
    print(df.head(n).to_string())
    
    print(f"\n{'='*60}")
    print("DATA SUMMARY")
    print(f"{'='*60}\n")
    print(f"Total rows: {len(df)}")
    print(f"\nColumns:")
    for col in df.columns:
        print(f"  - {col}: {df[col].notna().sum()} non-null values")

def main():
    """Main execution"""
    
    # File paths
    html_file = 'data/bronze/stocks/html/sgx_stocks.html'
    output_csv = 'data/bronze/stocks/sgx_stocks_extracted.csv'
    
    print("\n" + "="*60)
    print("SGX STOCK DATA EXTRACTOR")
    print("="*60)
    
    # Check if HTML file exists
    if not os.path.exists(html_file):
        print(f"\n✗ Error: File not found: {html_file}")
        print("\nPlease run stock_scrape.py first to generate the HTML file.")
        return None
    
    # Extract data
    df = extract_stocks_from_html(html_file)
    
    if df is None or len(df) == 0:
        print("\n✗ Failed to extract stock data")
        return None
    
    # Display sample
    display_sample(df)
    
    # Save to CSV
    save_to_csv(df, output_csv)
    
    print(f"\n{'='*60}")
    print("SUCCESS!")
    print(f"{'='*60}")
    print(f"\nExtracted {len(df)} stocks from Singapore Exchange")
    print(f"Data saved to: {output_csv}")
    
    return df

if __name__ == "__main__":
    df = main()
    
    if df is not None:
        print("\n\nYou can now use the CSV file for further analysis!")
        print("\nSample usage:")
        print("  import pandas as pd")
        print("  df = pd.read_csv('data/sgx_stocks_extracted.csv')")
        print("  print(df.head())")
