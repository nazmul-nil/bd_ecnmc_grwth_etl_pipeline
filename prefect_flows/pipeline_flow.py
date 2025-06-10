# -*- coding: utf-8 -*-
"""
Bangladesh Economic ETL Pipeline - Windows Compatible Prefect Flow
No emoji characters to avoid Windows encoding issues
@author: nazmu
"""

from prefect import flow, task
import subprocess
import os
import sys
from datetime import datetime
import pandas as pd
import sqlite3

# Add project root to path
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.append(BASE_DIR)

@task(name="ingest_data", retries=1)
def run_ingest():
    """Run data ingestion script"""
    print("TASK: Running data ingestion...")
    
    try:
        script_path = os.path.join(BASE_DIR, 'scripts', 'ingest_data.py')
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, cwd=BASE_DIR)
        
        if result.returncode == 0:
            print("SUCCESS: Data ingestion completed")
            
            # Check output
            api_dir = os.path.join(BASE_DIR, 'data', 'api')
            output_file = os.path.join(api_dir, 'bangladesh_wb_direct.csv')
            
            if os.path.exists(output_file):
                df = pd.read_csv(output_file)
                print(f"RESULT: Ingested {len(df)} records")
                return len(df)
            else:
                raise Exception("Output file not created")
        else:
            print(f"ERROR: Ingestion failed: {result.stderr}")
            raise Exception("Ingestion script failed")
            
    except Exception as e:
        print(f"ERROR: Ingestion error: {e}")
        raise

@task(name="transform_data", retries=1)
def run_transform(ingest_count):
    """Run data transformation script"""
    print("TASK: Running data transformation...")
    
    try:
        script_path = os.path.join(BASE_DIR, 'scripts', 'transform_data.py')
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, cwd=BASE_DIR)
        
        if result.returncode == 0:
            print("SUCCESS: Data transformation completed")
            
            # Check output
            processed_dir = os.path.join(BASE_DIR, 'data', 'processed')
            main_file = os.path.join(processed_dir, 'bangladesh_economic_indicators_processed.csv')
            
            if os.path.exists(main_file):
                df = pd.read_csv(main_file)
                print(f"RESULT: Transformed into {len(df)} records")
                return len(df)
            else:
                raise Exception("Processed file not created")
        else:
            print(f"ERROR: Transformation failed: {result.stderr}")
            raise Exception("Transformation script failed")
            
    except Exception as e:
        print(f"ERROR: Transformation error: {e}")
        raise

@task(name="load_data", retries=1) 
def run_load(transform_count):
    """Run data loading script"""
    print("TASK: Running data loading...")
    
    try:
        script_path = os.path.join(BASE_DIR, 'scripts', 'load_to_warehouse.py')
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, text=True, cwd=BASE_DIR)
        
        if result.returncode == 0:
            print("SUCCESS: Data loading completed")
            
            # Check warehouse
            warehouse_dir = os.path.join(BASE_DIR, 'data', 'warehouse')
            db_path = os.path.join(warehouse_dir, 'bangladesh_economic_data.db')
            
            if os.path.exists(db_path):
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM economic_indicators")
                record_count = cursor.fetchone()[0]
                conn.close()
                
                print(f"RESULT: Warehouse contains {record_count} records")
                return record_count
            else:
                raise Exception("Warehouse database not created")
        else:
            print(f"ERROR: Loading failed: {result.stderr}")
            raise Exception("Loading script failed")
            
    except Exception as e:
        print(f"ERROR: Loading error: {e}")
        raise

@task(name="validate_quality")
def validate_quality(load_count):
    """Validate data quality"""
    print("TASK: Validating data quality...")
    
    try:
        # Check warehouse
        warehouse_dir = os.path.join(BASE_DIR, 'data', 'warehouse')
        db_path = os.path.join(warehouse_dir, 'bangladesh_economic_data.db')
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Basic quality checks
        cursor.execute("SELECT COUNT(*) FROM economic_indicators")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM economic_indicators WHERE value IS NULL")
        null_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT indicator_name) FROM economic_indicators")
        indicator_count = cursor.fetchone()[0]
        
        conn.close()
        
        # Quality assessment
        quality_score = 100.0
        if null_count > 0:
            quality_score -= 20
        if indicator_count < 6:
            quality_score -= 20
        if total_records < 100:
            quality_score -= 10
            
        print(f"RESULT: Quality Score: {quality_score}%")
        print(f"  Total records: {total_records}")
        print(f"  Null values: {null_count}")
        print(f"  Indicators: {indicator_count}")
        
        return quality_score
        
    except Exception as e:
        print(f"ERROR: Quality validation error: {e}")
        raise

@flow(name="bangladesh_etl_simple")
def bangladesh_etl_pipeline():
    """
    Bangladesh ETL Pipeline Flow - Windows Compatible
    """
    print("STARTING: Bangladesh ETL Pipeline")
    print(f"TIMESTAMP: Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    try:
        # Step 1: Ingest data
        ingest_count = run_ingest()
        
        # Step 2: Transform data
        transform_count = run_transform(ingest_count)
        
        # Step 3: Load data
        load_count = run_load(transform_count)
        
        # Step 4: Validate quality
        quality_score = validate_quality(load_count)
        
        # Summary
        print("\n" + "=" * 50)
        print("SUCCESS: PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        print(f"SUMMARY: Pipeline Results:")
        print(f"  Ingested: {ingest_count} raw records")
        print(f"  Transformed: {transform_count} processed records")
        print(f"  Loaded: {load_count} warehouse records")
        print(f"  Quality Score: {quality_score}%")
        print(f"TIMESTAMP: Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
        return {
            "status": "SUCCESS",
            "ingest_count": ingest_count,
            "transform_count": transform_count,
            "load_count": load_count,
            "quality_score": quality_score
        }
        
    except Exception as e:
        print(f"\nERROR: PIPELINE FAILED: {e}")
        print("INFO: Check individual task outputs for details")
        raise

def main():
    """Main execution function"""
    print("SYSTEM: Bangladesh Economic ETL Pipeline Orchestration")
    print("MODE: Running in local mode (SQLite warehouse)")
    print()
    
    # Run the pipeline
    result = bangladesh_etl_pipeline()
    
    if result["status"] == "SUCCESS":
        print("\nSUCCESS: Pipeline execution successful!")
        print("NEXT STEPS:")
        print("  1. Explore your SQLite database in data/warehouse/")
        print("  2. Check exported files in data/exports/")
        print("  3. Use this for your portfolio demonstrations")
    
    return result

if __name__ == "__main__":
    main()