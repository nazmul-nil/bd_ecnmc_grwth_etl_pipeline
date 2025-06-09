# -*- coding: utf-8 -*-
"""
Bangladesh Economic Data Transformation Script
Transform raw World Bank API data into analysis-ready format
@author: nazmu
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Setup directories
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
API_DIR = os.path.join(BASE_DIR, 'data', 'api')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'processed')
os.makedirs(PROCESSED_DIR, exist_ok=True)

class BangladeshDataTransformer:
    """
    Data transformation class for Bangladesh economic indicators
    """
    
    def __init__(self, input_file="bangladesh_wb_direct.csv"):
        self.input_path = os.path.join(API_DIR, input_file)
        self.df = None
        self.processed_df = None
        
    def load_raw_data(self):
        """Load raw data from API ingestion"""
        try:
            print("📥 Loading raw data...")
            self.df = pd.read_csv(self.input_path)
            print(f"✅ Loaded {len(self.df)} records from {self.input_path}")
            print(f"📊 Data shape: {self.df.shape}")
            print(f"📅 Year range: {self.df['year'].min()} - {self.df['year'].max()}")
            return True
        except FileNotFoundError:
            print(f"❌ File not found: {self.input_path}")
            print("💡 Run ingest_data.py first to fetch the raw data")
            return False
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def validate_raw_data(self):
        """Validate raw data quality and structure"""
        print("\n🔍 Validating raw data quality...")
        
        # Check required columns
        required_cols = ['country_name', 'indicator_name', 'year', 'value']
        missing_cols = [col for col in required_cols if col not in self.df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Data quality checks
        total_records = len(self.df)
        null_values = self.df['value'].isnull().sum()
        null_percentage = (null_values / total_records) * 100
        
        print(f"📊 Data Quality Report:")
        print(f"   • Total records: {total_records:,}")
        print(f"   • Null values: {null_values:,} ({null_percentage:.1f}%)")
        print(f"   • Unique indicators: {self.df['indicator_name'].nunique()}")
        print(f"   • Year coverage: {self.df['year'].nunique()} years")
        
        # Check for duplicates
        duplicates = self.df.duplicated(subset=['indicator_name', 'year']).sum()
        if duplicates > 0:
            print(f"   ⚠️  Found {duplicates} duplicate records")
        else:
            print(f"   ✅ No duplicates found")
        
        # Validate year range
        if self.df['year'].min() < 1990 or self.df['year'].max() > 2030:
            print(f"   ⚠️  Unusual year range detected")
        else:
            print(f"   ✅ Year range looks valid")
    
    def clean_data(self):
        """Clean and standardize the raw data"""
        print("\n🧹 Cleaning data...")
        
        # Create a copy for processing
        df_clean = self.df.copy()
        original_count = len(df_clean)
        
        # Remove records with null values
        df_clean = df_clean.dropna(subset=['value'])
        print(f"   • Removed {original_count - len(df_clean)} records with null values")
        
        # Remove duplicates
        duplicates_before = df_clean.duplicated(subset=['indicator_name', 'year']).sum()
        df_clean = df_clean.drop_duplicates(subset=['indicator_name', 'year'])
        if duplicates_before > 0:
            print(f"   • Removed {duplicates_before} duplicate records")
        
        # Standardize country name
        df_clean['country_name'] = df_clean['country_name'].str.strip()
        df_clean['country_code'] = 'BGD'  # Ensure consistent country code
        
        # Clean indicator names (remove special characters, standardize)
        df_clean['indicator_name'] = df_clean['indicator_name'].str.strip()
        
        # Ensure year is integer
        df_clean['year'] = df_clean['year'].astype(int)
        
        # Sort by indicator and year
        df_clean = df_clean.sort_values(['indicator_name', 'year']).reset_index(drop=True)
        
        print(f"   ✅ Cleaned data: {len(df_clean)} records remaining")
        
        self.df_clean = df_clean
        return df_clean
    
    def create_features(self):
        """Create additional features and calculated metrics"""
        print("\n⚙️ Creating features and calculated metrics...")
        
        df_features = self.df_clean.copy()
        
        # Create year-over-year growth rates
        growth_indicators = ['gdp_per_capita', 'population', 'gdp_growth']
        
        for indicator in growth_indicators:
            indicator_data = df_features[df_features['indicator_name'] == indicator].copy()
            if len(indicator_data) > 1:
                indicator_data = indicator_data.sort_values('year')
                indicator_data[f'{indicator}_yoy_change'] = indicator_data['value'].pct_change() * 100
                indicator_data[f'{indicator}_yoy_abs_change'] = indicator_data['value'].diff()
                
                # Add these calculated metrics back to main dataframe
                for _, row in indicator_data.iterrows():
                    if not pd.isna(row[f'{indicator}_yoy_change']):
                        new_row = {
                            'country_name': row['country_name'],
                            'country_code': row['country_code'],
                            'indicator_code': f"CALC_{indicator.upper()}_YOY",
                            'indicator_name': f'{indicator}_yoy_growth_pct',
                            'year': row['year'],
                            'value': row[f'{indicator}_yoy_change']
                        }
                        df_features = pd.concat([df_features, pd.DataFrame([new_row])], ignore_index=True)
        
        # Create economic structure ratios
        df_pivot = df_features.pivot(index='year', columns='indicator_name', values='value')
        
        # Calculate economic diversification index (1 - sum of squares of sector shares)
        if all(col in df_pivot.columns for col in ['agriculture_pct_gdp', 'industry_pct_gdp']):
            # Estimate services as remainder (assuming they sum to ~100%)
            df_pivot['services_pct_gdp_estimated'] = 100 - df_pivot['agriculture_pct_gdp'] - df_pivot['industry_pct_gdp']
            
            # Calculate diversification index
            diversification_idx = 1 - (
                (df_pivot['agriculture_pct_gdp'] / 100) ** 2 +
                (df_pivot['industry_pct_gdp'] / 100) ** 2 +
                (df_pivot['services_pct_gdp_estimated'] / 100) ** 2
            )
            
            # Add diversification index to main dataframe
            for year, div_idx in diversification_idx.items():
                if not pd.isna(div_idx):
                    new_row = {
                        'country_name': 'Bangladesh',
                        'country_code': 'BGD',
                        'indicator_code': 'CALC_ECON_DIV_IDX',
                        'indicator_name': 'economic_diversification_index',
                        'year': year,
                        'value': div_idx
                    }
                    df_features = pd.concat([df_features, pd.DataFrame([new_row])], ignore_index=True)
        
        print(f"   ✅ Added calculated features. Total records: {len(df_features)}")
        
        self.df_features = df_features
        return df_features
    
    def create_time_series_analysis(self):
        """Create time series analysis features"""
        print("\n📈 Creating time series analysis features...")
        
        df_ts = self.df_features.copy()
        
        # Add trend indicators
        for indicator in df_ts['indicator_name'].unique():
            indicator_data = df_ts[df_ts['indicator_name'] == indicator].copy()
            
            if len(indicator_data) >= 5:  # Need at least 5 years for trend
                indicator_data = indicator_data.sort_values('year')
                
                # Calculate 3-year moving average
                indicator_data['moving_avg_3yr'] = indicator_data['value'].rolling(window=3, center=True).mean()
                
                # Calculate trend (simple linear trend coefficient)
                years = indicator_data['year'].values
                values = indicator_data['value'].values
                
                if len(years) > 1 and not any(pd.isna(values)):
                    trend_coef = np.polyfit(years, values, 1)[0]  # Linear trend coefficient
                    
                    # Add trend coefficient as a single record per indicator
                    trend_row = {
                        'country_name': 'Bangladesh',
                        'country_code': 'BGD',
                        'indicator_code': f"TREND_{indicator.upper()}",
                        'indicator_name': f'{indicator}_trend_coefficient',
                        'year': years[-1],  # Use latest year
                        'value': trend_coef
                    }
                    df_ts = pd.concat([df_ts, pd.DataFrame([trend_row])], ignore_index=True)
        
        print(f"   ✅ Added time series features. Total records: {len(df_ts)}")
        
        self.processed_df = df_ts
        return df_ts
    
    def generate_summary_statistics(self):
        """Generate summary statistics for quality assurance"""
        print("\n📊 Generating summary statistics...")
        
        # Summary by indicator
        summary_stats = []
        
        for indicator in self.processed_df['indicator_name'].unique():
            indicator_data = self.processed_df[self.processed_df['indicator_name'] == indicator]
            
            stats = {
                'indicator': indicator,
                'count': len(indicator_data),
                'min_year': indicator_data['year'].min(),
                'max_year': indicator_data['year'].max(),
                'mean_value': indicator_data['value'].mean(),
                'std_value': indicator_data['value'].std(),
                'min_value': indicator_data['value'].min(),
                'max_value': indicator_data['value'].max(),
                'null_count': indicator_data['value'].isnull().sum()
            }
            summary_stats.append(stats)
        
        summary_df = pd.DataFrame(summary_stats)
        summary_df = summary_df.round(3)
        
        print("📋 Summary Statistics by Indicator:")
        print(summary_df.to_string(index=False))
        
        return summary_df
    
    def save_processed_data(self):
        """Save processed data to CSV files"""
        print("\n💾 Saving processed data...")
        
        # Save main processed dataset
        main_output_path = os.path.join(PROCESSED_DIR, "bangladesh_economic_indicators_processed.csv")
        self.processed_df.to_csv(main_output_path, index=False)
        print(f"   ✅ Saved main dataset: {main_output_path}")
        
        # Save wide format for analysis (pivot table)
        pivot_df = self.processed_df.pivot(index='year', columns='indicator_name', values='value')
        pivot_df = pivot_df.reset_index()
        
        wide_output_path = os.path.join(PROCESSED_DIR, "bangladesh_economic_indicators_wide.csv")
        pivot_df.to_csv(wide_output_path, index=False)
        print(f"   ✅ Saved wide format: {wide_output_path}")
        
        # Save summary statistics
        summary_stats = self.generate_summary_statistics()
        summary_output_path = os.path.join(PROCESSED_DIR, "bangladesh_data_summary.csv")
        summary_stats.to_csv(summary_output_path, index=False)
        print(f"   ✅ Saved summary stats: {summary_output_path}")
        
        return {
            'main_dataset': main_output_path,
            'wide_format': wide_output_path,
            'summary_stats': summary_output_path
        }
    
    def run_transformation(self):
        """Run the complete transformation pipeline"""
        print("🔄 Starting Bangladesh Economic Data Transformation Pipeline")
        print("=" * 60)
        
        # Step 1: Load raw data
        if not self.load_raw_data():
            return False
        
        # Step 2: Validate data
        self.validate_raw_data()
        
        # Step 3: Clean data
        self.clean_data()
        
        # Step 4: Create features
        self.create_features()
        
        # Step 5: Time series analysis
        self.create_time_series_analysis()
        
        # Step 6: Save processed data
        output_files = self.save_processed_data()
        
        print("\n🎉 Transformation Complete!")
        print("=" * 60)
        print(f"📊 Final dataset: {len(self.processed_df)} records")
        print(f"📈 Indicators: {self.processed_df['indicator_name'].nunique()}")
        print(f"📅 Years covered: {self.processed_df['year'].min()} - {self.processed_df['year'].max()}")
        print("\n📁 Output files created:")
        for file_type, file_path in output_files.items():
            print(f"   • {file_type}: {os.path.basename(file_path)}")
        
        return True

def main():
    """Main execution function"""
    transformer = BangladeshDataTransformer()
    success = transformer.run_transformation()
    
    if success:
        print("\n✅ Data transformation successful!")
        print("📋 Next steps:")
        print("   1. Review processed data files in data/processed/")
        print("   2. Run load_to_s3.py to upload to cloud storage")
        print("   3. Set up Prefect workflow for automation")
    else:
        print("\n❌ Data transformation failed!")
        print("💡 Check error messages above and ensure ingest_data.py was run first")

if __name__ == "__main__":
    main()

