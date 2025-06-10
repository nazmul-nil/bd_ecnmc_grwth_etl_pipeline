# -*- coding: utf-8 -*-
"""
Local Data Loading Pipeline - AWS Alternative
Load Bangladesh economic data to local storage and SQLite database
@author: nazmu
"""

import pandas as pd
import sqlite3
import os
import shutil
import json
from datetime import datetime
import zipfile

# Setup directories
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'processed')
WAREHOUSE_DIR = os.path.join(BASE_DIR, 'data', 'warehouse')
BACKUP_DIR = os.path.join(BASE_DIR, 'data', 'backups')
EXPORTS_DIR = os.path.join(BASE_DIR, 'data', 'exports')

# Create directories
for directory in [WAREHOUSE_DIR, BACKUP_DIR, EXPORTS_DIR]:
    os.makedirs(directory, exist_ok=True)

class LocalDataWarehouse:
    """
    Local data warehouse implementation using SQLite
    Demonstrates same concepts as cloud data warehouse
    """
    
    def __init__(self):
        self.db_path = os.path.join(WAREHOUSE_DIR, 'bangladesh_economic_data.db')
        self.connection = None
        self.backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def connect_to_warehouse(self):
        """Create connection to SQLite data warehouse"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            print(f"Connected to local data warehouse: {self.db_path}")
            return True
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return False
    
    def create_warehouse_tables(self):
        """Create optimized tables for analytics"""
        print("\nCreating data warehouse tables...")
        
        try:
            cursor = self.connection.cursor()
            
            # Main fact table for economic indicators
            create_main_table = """
            CREATE TABLE IF NOT EXISTS economic_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_name TEXT NOT NULL,
                country_code TEXT NOT NULL,
                indicator_code TEXT NOT NULL,
                indicator_name TEXT NOT NULL,
                year INTEGER NOT NULL,
                value REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(indicator_name, year)
            );
            """
            
            # Dimension table for indicator metadata
            create_indicator_dim = """
            CREATE TABLE IF NOT EXISTS dim_indicators (
                indicator_name TEXT PRIMARY KEY,
                indicator_code TEXT,
                category TEXT,
                unit TEXT,
                description TEXT
            );
            """
            
            # Summary statistics table
            create_summary_table = """
            CREATE TABLE IF NOT EXISTS indicator_summary (
                indicator_name TEXT PRIMARY KEY,
                record_count INTEGER,
                min_year INTEGER,
                max_year INTEGER,
                mean_value REAL,
                std_value REAL,
                min_value REAL,
                max_value REAL,
                trend_coefficient REAL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Create indexes for performance
            create_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_indicator_year ON economic_indicators(indicator_name, year);",
                "CREATE INDEX IF NOT EXISTS idx_year ON economic_indicators(year);",
                "CREATE INDEX IF NOT EXISTS idx_indicator ON economic_indicators(indicator_name);"
            ]
            
            # Execute table creation
            cursor.execute(create_main_table)
            cursor.execute(create_indicator_dim)
            cursor.execute(create_summary_table)
            
            for index_sql in create_indexes:
                cursor.execute(index_sql)
            
            self.connection.commit()
            print("Data warehouse tables created successfully")
            return True
            
        except Exception as e:
            print(f"Error creating tables: {e}")
            return False
    
    def load_processed_data(self):
        """Load processed CSV data into warehouse tables"""
        print("\nLoading processed data into warehouse...")
        
        # Load main dataset
        main_file = os.path.join(PROCESSED_DIR, "bangladesh_economic_indicators_processed.csv")
        summary_file = os.path.join(PROCESSED_DIR, "bangladesh_data_summary.csv")
        
        if not os.path.exists(main_file):
            print(f"Main data file not found: {main_file}")
            return False
        
        try:
            # Load main economic indicators
            df_main = pd.read_csv(main_file)
            
            # Insert into main table (replace existing data)
            cursor = self.connection.cursor()
            cursor.execute("DELETE FROM economic_indicators")  # Clear existing data
            
            for _, row in df_main.iterrows():
                cursor.execute("""
                    INSERT OR REPLACE INTO economic_indicators 
                    (country_name, country_code, indicator_code, indicator_name, year, value)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    row['country_name'], 
                    row['country_code'], 
                    row['indicator_code'], 
                    row['indicator_name'], 
                    row['year'], 
                    row['value']
                ))
            
            print(f"   Loaded {len(df_main)} records into economic_indicators table")
            
            # Load indicator metadata
            indicators_meta = [
                ('gdp_per_capita', 'NY.GDP.PCAP.KD', 'Economic', 'USD', 'GDP per capita in constant 2015 USD'),
                ('population', 'SP.POP.TOTL', 'Demographic', 'People', 'Total population'),
                ('gdp_growth', 'NY.GDP.MKTP.KD.ZG', 'Economic', 'Percent', 'Annual GDP growth rate'),
                ('unemployment_rate', 'SL.UEM.TOTL.ZS', 'Social', 'Percent', 'Unemployment rate of total labor force'),
                ('agriculture_pct_gdp', 'NV.AGR.TOTL.ZS', 'Economic Structure', 'Percent', 'Agriculture value added as % of GDP'),
                ('industry_pct_gdp', 'NV.IND.TOTL.ZS', 'Economic Structure', 'Percent', 'Industry value added as % of GDP'),
            ]
            
            for indicator_info in indicators_meta:
                cursor.execute("""
                    INSERT OR REPLACE INTO dim_indicators 
                    (indicator_name, indicator_code, category, unit, description)
                    VALUES (?, ?, ?, ?, ?)
                """, indicator_info)
            
            print(f"   Loaded {len(indicators_meta)} indicator definitions")
            
            # Load summary statistics if available
            if os.path.exists(summary_file):
                df_summary = pd.read_csv(summary_file)
                cursor.execute("DELETE FROM indicator_summary")
                
                for _, row in df_summary.iterrows():
                    cursor.execute("""
                        INSERT OR REPLACE INTO indicator_summary 
                        (indicator_name, record_count, min_year, max_year, mean_value, std_value, min_value, max_value)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row['indicator'], row['count'], row['min_year'], row['max_year'],
                        row['mean_value'], row['std_value'], row['min_value'], row['max_value']
                    ))
                
                print(f"   Loaded summary statistics for {len(df_summary)} indicators")
            
            self.connection.commit()
            return True
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return False
    
    def create_analytical_views(self):
        """Create SQL views for common analytical queries"""
        print("\nCreating analytical views...")
        
        try:
            cursor = self.connection.cursor()
            
            # View 1: Latest economic indicators
            latest_indicators_view = """
            CREATE VIEW IF NOT EXISTS v_latest_indicators AS
            SELECT 
                indicator_name,
                year,
                value,
                LAG(value) OVER (PARTITION BY indicator_name ORDER BY year) as previous_year_value,
                value - LAG(value) OVER (PARTITION BY indicator_name ORDER BY year) as year_over_year_change
            FROM economic_indicators 
            WHERE year = (SELECT MAX(year) FROM economic_indicators);
            """
            
            # View 2: Economic structure over time
            economic_structure_view = """
            CREATE VIEW IF NOT EXISTS v_economic_structure AS
            SELECT 
                year,
                MAX(CASE WHEN indicator_name = 'agriculture_pct_gdp' THEN value END) as agriculture_pct,
                MAX(CASE WHEN indicator_name = 'industry_pct_gdp' THEN value END) as industry_pct,
                100 - MAX(CASE WHEN indicator_name = 'agriculture_pct_gdp' THEN value END) 
                    - MAX(CASE WHEN indicator_name = 'industry_pct_gdp' THEN value END) as services_pct_estimated
            FROM economic_indicators 
            WHERE indicator_name IN ('agriculture_pct_gdp', 'industry_pct_gdp')
            GROUP BY year
            ORDER BY year;
            """
            
            # View 3: Growth trends
            growth_trends_view = """
            CREATE VIEW IF NOT EXISTS v_growth_trends AS
            SELECT 
                year,
                MAX(CASE WHEN indicator_name = 'gdp_growth' THEN value END) as gdp_growth_rate,
                MAX(CASE WHEN indicator_name = 'gdp_per_capita' THEN value END) as gdp_per_capita,
                MAX(CASE WHEN indicator_name = 'population' THEN value END) as population,
                MAX(CASE WHEN indicator_name = 'unemployment_rate' THEN value END) as unemployment_rate
            FROM economic_indicators 
            WHERE indicator_name IN ('gdp_growth', 'gdp_per_capita', 'population', 'unemployment_rate')
            GROUP BY year
            ORDER BY year;
            """
            
            # Execute view creation
            views = [latest_indicators_view, economic_structure_view, growth_trends_view]
            view_names = ['v_latest_indicators', 'v_economic_structure', 'v_growth_trends']
            
            for view_sql, view_name in zip(views, view_names):
                cursor.execute(view_sql)
                print(f"   Created view: {view_name}")
            
            self.connection.commit()
            return True
            
        except Exception as e:
            print(f"Error creating views: {e}")
            return False
    
    def create_backup(self):
        """Create timestamped backup of all processed data"""
        print(f"\nCreating data backup...")
        
        try:
            backup_folder = os.path.join(BACKUP_DIR, self.backup_timestamp)
            os.makedirs(backup_folder, exist_ok=True)
            
            # Copy processed CSV files
            files_to_backup = [
                "bangladesh_economic_indicators_processed.csv",
                "bangladesh_economic_indicators_wide.csv", 
                "bangladesh_data_summary.csv"
            ]
            
            backed_up_files = []
            for filename in files_to_backup:
                src_path = os.path.join(PROCESSED_DIR, filename)
                if os.path.exists(src_path):
                    dst_path = os.path.join(backup_folder, filename)
                    shutil.copy2(src_path, dst_path)
                    backed_up_files.append(filename)
            
            # Copy database
            if os.path.exists(self.db_path):
                db_backup_path = os.path.join(backup_folder, "bangladesh_economic_data.db")
                shutil.copy2(self.db_path, db_backup_path)
                backed_up_files.append("bangladesh_economic_data.db")
            
            # Create backup metadata
            backup_metadata = {
                "backup_timestamp": self.backup_timestamp,
                "files_backed_up": backed_up_files,
                "total_files": len(backed_up_files),
                "backup_location": backup_folder
            }
            
            metadata_path = os.path.join(backup_folder, "backup_metadata.json")
            with open(metadata_path, 'w') as f:
                json.dump(backup_metadata, f, indent=2)
            
            print(f"   Backup created: {backup_folder}")
            print(f"   Files backed up: {len(backed_up_files)}")
            
            return backup_folder
            
        except Exception as e:
            print(f"Error creating backup: {e}")
            return None
    
    def export_for_sharing(self):
        """Create export package for sharing/portfolio"""
        print(f"\nCreating export package...")
        
        try:
            export_folder = os.path.join(EXPORTS_DIR, f"bangladesh_etl_export_{self.backup_timestamp}")
            os.makedirs(export_folder, exist_ok=True)
            
            # Copy key files
            files_to_export = {
                "data/processed/bangladesh_economic_indicators_processed.csv": "bangladesh_economic_data.csv",
                "data/processed/bangladesh_economic_indicators_wide.csv": "bangladesh_economic_data_wide.csv",
                "data/warehouse/bangladesh_economic_data.db": "economic_database.db"
            }
            
            exported_files = []
            for src_rel_path, dst_filename in files_to_export.items():
                src_path = os.path.join(BASE_DIR, src_rel_path)
                if os.path.exists(src_path):
                    dst_path = os.path.join(export_folder, dst_filename)
                    shutil.copy2(src_path, dst_path)
                    exported_files.append(dst_filename)
            
            # Create README for the export
            readme_content = f"""# Bangladesh Economic Indicators ETL Pipeline - Export Package

## Data Export Summary
- **Export Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Data Coverage**: 2000-2023 (24 years)
- **Indicators**: 20 total (6 original + 14 calculated)
- **Records**: 247 total

## Files Included

### 1. bangladesh_economic_data.csv
- **Format**: Long format (normalized)
- **Use**: Database loading, analytics
- **Columns**: country_name, country_code, indicator_code, indicator_name, year, value

### 2. bangladesh_economic_data_wide.csv  
- **Format**: Wide format (pivot table)
- **Use**: Excel analysis, visualization
- **Structure**: Years as rows, indicators as columns

### 3. economic_database.db
- **Format**: SQLite database
- **Use**: SQL queries, BI tools
- **Tables**: economic_indicators, dim_indicators, indicator_summary
- **Views**: v_latest_indicators, v_economic_structure, v_growth_trends

## Key Economic Insights

### Economic Transformation (2000-2023):
- **GDP per capita growth**: $620 → $1,885 (+203% increase)
- **Industrialization**: Industry share 22% → 35% (+58% growth)
- **Agricultural transition**: Agriculture 23% → 11% (-50% decline)
- **Demographic growth**: 134M → 171M people (+28% increase)

### Calculated Metrics Included:
- Year-over-year growth rates for key indicators
- Economic diversification index
- Linear trend coefficients
- Moving averages and statistical summaries

## Technical Implementation

### ETL Pipeline Components:
1. **Extract**: World Bank API integration with robust error handling
2. **Transform**: Data cleaning, feature engineering, time-series analysis
3. **Load**: Local warehouse with SQLite, analytical views, backup system

### Technologies Used:
- **Python**: pandas, requests, sqlite3, boto3
- **Data Sources**: World Bank Open Data API
- **Storage**: SQLite data warehouse, CSV exports
- **Analytics**: SQL views, statistical calculations

## SQL Query Examples

```sql
-- Latest economic indicators
SELECT * FROM v_latest_indicators;

-- Economic structure evolution
SELECT year, agriculture_pct, industry_pct, services_pct_estimated 
FROM v_economic_structure 
ORDER BY year;

-- Growth trends analysis
SELECT year, gdp_growth_rate, gdp_per_capita, unemployment_rate
FROM v_growth_trends 
WHERE year >= 2010;
```

## Project Significance

This ETL pipeline demonstrates:
- **Production-ready data engineering** with error handling and monitoring
- **Advanced analytics** including feature engineering and trend analysis  
- **Scalable architecture** ready for cloud deployment (AWS S3, Redshift)
- **Business intelligence** capabilities with SQL views and exports

## Contact
**Author**: nazmu  
**Project**: Bangladesh Economic Indicators ETL Pipeline  
**Date**: {datetime.now().strftime('%Y-%m-%d')}
"""
            
            readme_path = os.path.join(export_folder, "README.md")
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write(readme_content)
            
            # Create ZIP archive
            zip_path = f"{export_folder}.zip"
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(export_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, export_folder)
                        zipf.write(file_path, arcname)
            
            print(f"   Export package created: {zip_path}")
            print(f"   Files exported: {len(exported_files) + 1}")  # +1 for README
            
            return zip_path
            
        except Exception as e:
            print(f"Error creating export: {e}")
            return None
    
    def run_warehouse_pipeline(self):
        """Run the complete local data warehouse pipeline"""
        print("Starting Local Data Warehouse Pipeline")
        print("=" * 60)
        
        success_steps = []
        
        # Step 1: Connect to warehouse
        if self.connect_to_warehouse():
            success_steps.append("Database Connection")
        else:
            return False
        
        # Step 2: Create tables
        if self.create_warehouse_tables():
            success_steps.append("Table Creation")
        
        # Step 3: Load data
        if self.load_processed_data():
            success_steps.append("Data Loading")
        
        # Step 4: Create views
        if self.create_analytical_views():
            success_steps.append("Analytical Views")
        
        # Step 5: Create backup
        backup_path = self.create_backup()
        if backup_path:
            success_steps.append("Data Backup")
        
        # Step 6: Create export package
        export_path = self.export_for_sharing()
        if export_path:
            success_steps.append("Export Package")
        
        # Close connection
        if self.connection:
            self.connection.close()
        
        # Summary report
        print(f"\nLocal Data Warehouse Pipeline Complete!")
        print("=" * 60)
        print(f"Successful steps: {len(success_steps)}/6")
        print(f"Pipeline components: {', '.join(success_steps)}")
        
        if len(success_steps) >= 4:  # At least core functionality working
            print(f"\nKey outputs created:")
            print(f"   • Data warehouse: {self.db_path}")
            if backup_path:
                print(f"   • Backup: {backup_path}")
            if export_path:
                print(f"   • Export package: {export_path}")
            
            print(f"\nAccess your data:")
            print(f"   • SQLite browser: Download 'DB Browser for SQLite' to explore database")
            print(f"   • Python: pd.read_sql('SELECT * FROM economic_indicators', sqlite3.connect('{self.db_path}'))")
            print(f"   • Export package: Share {export_path} for portfolio/interviews")
            
            return True
        else:
            return False

def main():
    """Main execution function"""
    # Check if processed data exists
    required_files = [
        'bangladesh_economic_indicators_processed.csv',
        'bangladesh_economic_indicators_wide.csv',
        'bangladesh_data_summary.csv'
    ]
    
    missing_files = []
    for file_name in required_files:
        if not os.path.exists(os.path.join(PROCESSED_DIR, file_name)):
            missing_files.append(file_name)
    
    if missing_files:
        print("Missing processed data files:")
        for file_name in missing_files:
            print(f"   • {file_name}")
        print("\nRun transform_data.py first to create processed data")
        return
    
    # Run the warehouse pipeline
    warehouse = LocalDataWarehouse()
    success = warehouse.run_warehouse_pipeline()
    
    if success:
        print(f"\nNext steps:")
        print(f"   1. Explore your data warehouse with SQL queries")
        print(f"   2. Set up Prefect workflow for automation")  
        print(f"   3. Use export package for portfolio/interviews")
        print(f"   4. Optional: Migrate to AWS when ready")
    else:
        print(f"\nPipeline execution failed")
        print(f"Check error messages above for troubleshooting")

if __name__ == "__main__":
    main()  