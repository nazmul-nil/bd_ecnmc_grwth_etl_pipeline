# -*- coding: utf-8 -*-
"""
Load Bangladesh Economic Data to AWS S3 and Redshift
Upload processed data to cloud storage and data warehouse
@author: nazmu
"""

import boto3
import pandas as pd
import os
import json
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
import psycopg2
from sqlalchemy import create_engine
import configparser

# Setup directories
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'processed')
CONFIG_DIR = os.path.join(BASE_DIR, 'config')
os.makedirs(CONFIG_DIR, exist_ok=True)

class AWSDataLoader:
    """
    AWS S3 and Redshift data loader for Bangladesh economic indicators
    """
    
    def __init__(self):
        self.s3_client = None
        self.redshift_engine = None
        self.bucket_name = "bangladesh-economic-data"  # Change this to your bucket name
        self.config = self.load_config()
        
    def load_config(self):
        """Load AWS configuration from config file or environment variables"""
        config = configparser.ConfigParser()
        config_file = os.path.join(CONFIG_DIR, 'aws_config.ini')
        
        # Create sample config file if it doesn't exist
        if not os.path.exists(config_file):
            self.create_sample_config(config_file)
            print(f"Created sample config file: {config_file}")
            print("Please update with your AWS credentials before running")
        
        config.read(config_file)
        return config
    
    def create_sample_config(self, config_file):
        """Create a sample configuration file"""
        sample_config = """[AWS]
# AWS Access Credentials (get from AWS IAM)
aws_access_key_id = YOUR_ACCESS_KEY_HERE
aws_secret_access_key = YOUR_SECRET_KEY_HERE
aws_region = us-east-1

[S3]
# S3 Bucket Configuration
bucket_name = bangladesh-economic-data
data_prefix = processed-data/
backup_prefix = backups/

[REDSHIFT]
# Redshift Data Warehouse Configuration (optional)
host = your-cluster.redshift.amazonaws.com
port = 5439
database = bangladesh_db
username = your_username
password = your_password
schema = public

[SETTINGS]
# Pipeline Settings
upload_backup = true
create_redshift_tables = false
"""
        with open(config_file, 'w') as f:
            f.write(sample_config)
    
    def setup_aws_connection(self):
        """Setup AWS S3 connection"""
        try:
            # Try to get credentials from config first
            if self.config.has_section('AWS'):
                aws_access_key = self.config.get('AWS', 'aws_access_key_id', fallback=None)
                aws_secret_key = self.config.get('AWS', 'aws_secret_access_key', fallback=None)
                aws_region = self.config.get('AWS', 'aws_region', fallback='us-east-1')
                
                if aws_access_key and aws_access_key != 'YOUR_ACCESS_KEY_HERE':
                    self.s3_client = boto3.client(
                        's3',
                        aws_access_key_id=aws_access_key,
                        aws_secret_access_key=aws_secret_key,
                        region_name=aws_region
                    )
                else:
                    # Fall back to default credentials (AWS CLI, IAM roles, etc.)
                    self.s3_client = boto3.client('s3')
            else:
                self.s3_client = boto3.client('s3')
            
            print("AWS S3 connection established")
            return True
            
        except NoCredentialsError:
            print("AWS credentials not found")
            print("Please configure AWS credentials using one of these methods:")
            print("   1. Update config/aws_config.ini with your credentials")
            print("   2. Run 'aws configure' if you have AWS CLI installed")
            print("   3. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
            return False
        except Exception as e:
            print(f"Error setting up AWS connection: {e}")
            return False
    
    def create_s3_bucket(self):
        """Create S3 bucket if it doesn't exist"""
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"S3 bucket '{self.bucket_name}' already exists")
            return True
            
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            
            if error_code == 404:
                # Bucket doesn't exist, create it
                try:
                    print(f"Creating S3 bucket: {self.bucket_name}")
                    
                    # Note: For regions other than us-east-1, you need to specify LocationConstraint
                    region = self.config.get('AWS', 'aws_region', fallback='us-east-1')
                    
                    if region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': region}
                        )
                    
                    print(f"S3 bucket '{self.bucket_name}' created successfully")
                    return True
                    
                except ClientError as create_error:
                    print(f"Error creating S3 bucket: {create_error}")
                    return False
            else:
                print(f"Error accessing S3 bucket: {e}")
                return False
    
    def upload_file_to_s3(self, file_path, s3_key, metadata=None):
        """Upload a file to S3 with metadata"""
        try:
            # Prepare extra args with metadata
            extra_args = {
                'ContentType': 'text/csv',
                'ServerSideEncryption': 'AES256'  # Server-side encryption
            }
            
            if metadata:
                extra_args['Metadata'] = metadata
            
            # Upload file
            print(f"Uploading {os.path.basename(file_path)} to s3://{self.bucket_name}/{s3_key}")
            
            self.s3_client.upload_file(
                file_path, 
                self.bucket_name, 
                s3_key,
                ExtraArgs=extra_args
            )
            
            print(f"Successfully uploaded to S3")
            return True
            
        except Exception as e:
            print(f"Error uploading {file_path}: {e}")
            return False
    
    def upload_processed_data(self):
        """Upload all processed data files to S3"""
        print("\nUploading processed data to S3...")
        
        # Get current timestamp for versioning
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Define files to upload
        files_to_upload = [
            {
                'local_file': 'bangladesh_economic_indicators_processed.csv',
                's3_key': f'processed-data/bangladesh_economic_indicators_processed.csv',
                'backup_key': f'backups/{timestamp}/bangladesh_economic_indicators_processed.csv',
                'description': 'Main processed dataset (long format)'
            },
            {
                'local_file': 'bangladesh_economic_indicators_wide.csv',
                's3_key': f'processed-data/bangladesh_economic_indicators_wide.csv',
                'backup_key': f'backups/{timestamp}/bangladesh_economic_indicators_wide.csv',
                'description': 'Wide format dataset for analysis'
            },
            {
                'local_file': 'bangladesh_data_summary.csv',
                's3_key': f'processed-data/bangladesh_data_summary.csv',
                'backup_key': f'backups/{timestamp}/bangladesh_data_summary.csv',
                'description': 'Summary statistics and data quality report'
            }
        ]
        
        upload_results = []
        
        for file_info in files_to_upload:
            local_path = os.path.join(PROCESSED_DIR, file_info['local_file'])
            
            if os.path.exists(local_path):
                # Prepare metadata
                file_stats = os.stat(local_path)
                metadata = {
                    'source': 'bangladesh-etl-pipeline',
                    'upload_timestamp': timestamp,
                    'file_size_bytes': str(file_stats.st_size),
                    'description': file_info['description']
                }
                
                # Upload main file
                success = self.upload_file_to_s3(local_path, file_info['s3_key'], metadata)
                
                # Upload backup if enabled
                if success and self.config.getboolean('SETTINGS', 'upload_backup', fallback=True):
                    backup_success = self.upload_file_to_s3(local_path, file_info['backup_key'], metadata)
                    if backup_success:
                        print(f"   Backup created: {file_info['backup_key']}")
                
                upload_results.append({
                    'file': file_info['local_file'],
                    'success': success,
                    's3_url': f"s3://{self.bucket_name}/{file_info['s3_key']}"
                })
            else:
                print(f"File not found: {local_path}")
                upload_results.append({
                    'file': file_info['local_file'],
                    'success': False,
                    'error': 'File not found'
                })
        
        return upload_results
    
    def create_redshift_tables(self):
        """Create Redshift tables for the data (optional)"""
        print("\nCreating Redshift tables...")
        
        if not self.config.has_section('REDSHIFT'):
            print("Redshift configuration not found, skipping table creation")
            return False
        
        try:
            # Create connection string
            host = self.config.get('REDSHIFT', 'host')
            port = self.config.get('REDSHIFT', 'port', fallback='5439')
            database = self.config.get('REDSHIFT', 'database')
            username = self.config.get('REDSHIFT', 'username')
            password = self.config.get('REDSHIFT', 'password')
            
            connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            self.redshift_engine = create_engine(connection_string)
            
            # SQL to create main table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS bangladesh_economic_indicators (
                country_name VARCHAR(100),
                country_code VARCHAR(10),
                indicator_code VARCHAR(100),
                indicator_name VARCHAR(200),
                year INTEGER,
                value DECIMAL(18,6),
                upload_date TIMESTAMP DEFAULT GETDATE()
            );
            """
            
            # Execute table creation
            with self.redshift_engine.connect() as conn:
                conn.execute(create_table_sql)
                print("Redshift table created/verified")
            
            return True
            
        except Exception as e:
            print(f"Error setting up Redshift: {e}")
            print("Redshift setup is optional - your data is safely stored in S3")
            return False
    
    def generate_upload_report(self, upload_results):
        """Generate a summary report of the upload process"""
        print("\nUpload Summary Report")
        print("=" * 50)
        
        successful_uploads = [r for r in upload_results if r['success']]
        failed_uploads = [r for r in upload_results if not r['success']]
        
        print(f"Successful uploads: {len(successful_uploads)}")
        print(f"Failed uploads: {len(failed_uploads)}")
        
        if successful_uploads:
            print("\nSuccessfully uploaded files:")
            for result in successful_uploads:
                print(f"   • {result['file']} → {result['s3_url']}")
        
        if failed_uploads:
            print("\nFailed uploads:")
            for result in failed_uploads:
                error_msg = result.get('error', 'Unknown error')
                print(f"   • {result['file']}: {error_msg}")
        
        # Generate S3 access instructions
        if successful_uploads:
            print(f"\nAccess your data:")
            print(f"   • S3 Console: https://s3.console.aws.amazon.com/s3/buckets/{self.bucket_name}")
            print(f"   • AWS CLI: aws s3 ls s3://{self.bucket_name}/processed-data/")
            print(f"   • Boto3: s3_client.download_file('{self.bucket_name}', 'processed-data/file.csv', 'local_file.csv')")
    
    def run_loading_pipeline(self):
        """Run the complete data loading pipeline"""
        print("Starting AWS Data Loading Pipeline")
        print("=" * 60)
        
        # Step 1: Setup AWS connection
        if not self.setup_aws_connection():
            return False
        
        # Step 2: Create/verify S3 bucket
        if not self.create_s3_bucket():
            return False
        
        # Step 3: Upload processed data
        upload_results = self.upload_processed_data()
        
        # Step 4: Create Redshift tables (optional)
        if self.config.getboolean('SETTINGS', 'create_redshift_tables', fallback=False):
            self.create_redshift_tables()
        
        # Step 5: Generate report
        self.generate_upload_report(upload_results)
        
        # Check if any uploads were successful
        successful_uploads = [r for r in upload_results if r['success']]
        
        if successful_uploads:
            print(f"\nData loading pipeline completed successfully!")
            print(f"{len(successful_uploads)} files uploaded to AWS S3")
            print("\nNext steps:")
            print("   1. Verify data in S3 console")
            print("   2. Set up Prefect workflow for automation")
            print("   3. Connect BI tools (Tableau, Power BI) to S3/Redshift")
            return True
        else:
            print(f"\nData loading pipeline failed!")
            print("Check AWS credentials and permissions")
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
    
    # Run the loading pipeline
    loader = AWSDataLoader()
    success = loader.run_loading_pipeline()
    
    if not success:
        print("\nTroubleshooting tips:")
        print("   1. Ensure AWS credentials are configured")
        print("   2. Check your AWS IAM permissions for S3")
        print("   3. Verify your S3 bucket name is globally unique")
        print("   4. Update config/aws_config.ini with your settings")

if __name__ == "__main__":
    main()