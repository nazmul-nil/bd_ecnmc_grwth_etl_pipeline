# -*- coding: utf-8 -*-
"""
Bangladesh Economic Data Ingestion using direct World Bank API calls
Backup approach if wbdata library has issues
@author: nazmu
"""

import requests
import pandas as pd
import json
import os
import time

# Setup output folder
API_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'api')
os.makedirs(API_DIR, exist_ok=True)

# World Bank indicators
indicators = {
    "NY.GDP.PCAP.KD": "gdp_per_capita",
    "SP.POP.TOTL": "population",
    "NY.GDP.MKTP.KD.ZG": "gdp_growth", 
    "SL.UEM.TOTL.ZS": "unemployment_rate",
    "NV.AGR.TOTL.ZS": "agriculture_pct_gdp",
    "NV.IND.TOTL.ZS": "industry_pct_gdp"
}

def fetch_wb_indicator(indicator_code, country_code="BGD", start_year=2000, end_year=2023):
    """
    Fetch a single indicator from World Bank API using direct HTTP requests
    """
    base_url = "https://api.worldbank.org/v2/country"
    url = f"{base_url}/{country_code}/indicator/{indicator_code}"
    
    params = {
        'format': 'json',
        'date': f"{start_year}:{end_year}",
        'per_page': 100  # Adjust if needed
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # World Bank API returns [metadata, data] format
        if len(data) > 1 and data[1]:
            return data[1]  # Return the actual data part
        else:
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"    ❌ HTTP Error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"    ❌ JSON Error: {e}")
        return None

def main():
    """
    Fetch Bangladesh economic indicators using direct API calls
    """
    print("📡 Fetching Bangladesh data from World Bank API (direct HTTP)...")
    
    all_records = []
    
    for indicator_code, indicator_name in indicators.items():
        print(f"  📊 Fetching {indicator_name} ({indicator_code})...")
        
        data = fetch_wb_indicator(indicator_code)
        
        if data:
            for record in data:
                if record.get('value') is not None:
                    all_records.append({
                        'country_name': record.get('country', {}).get('value', 'Bangladesh'),
                        'country_code': record.get('country', {}).get('id', 'BGD'),
                        'indicator_code': indicator_code,
                        'indicator_name': indicator_name,
                        'year': int(record.get('date')) if record.get('date') else None,
                        'value': record.get('value')
                    })
            
            print(f"    ✅ Success: {len([r for r in data if r.get('value') is not None])} valid records")
        else:
            print(f"    ⚠️  No data returned for {indicator_name}")
        
        time.sleep(0.5)  # Be respectful to the API
    
    if not all_records:
        print("❌ No data was fetched successfully")
        return
    
    # Create DataFrame
    print(f"\n🔄 Processing {len(all_records)} records...")
    df = pd.DataFrame(all_records)
    
    # Clean and sort
    df = df.dropna(subset=['value', 'year'])
    df = df.sort_values(['indicator_name', 'year'])
    
    # Save to CSV
    output_path = os.path.join(API_DIR, "bangladesh_wb_direct.csv")
    df.to_csv(output_path, index=False)
    
    # Print summary
    print(f"✅ Saved {len(df)} records to: {output_path}")
    print(f"📊 Coverage: {df['year'].min()}-{df['year'].max()}")
    print(f"📈 Indicators: {df['indicator_name'].nunique()}")
    
    print("\n📋 Sample data:")
    print(df.head(8))
    
    print(f"\n📊 Records per indicator:")
    indicator_counts = df.groupby('indicator_name').size().sort_values(ascending=False)
    print(indicator_counts)

if __name__ == "__main__":
    main()