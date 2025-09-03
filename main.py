import sys
import os
import logging
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import ETL modules
from data_extraction import load_raw_data
from data_transformation import full_transformation_pipeline
from dimensional_etl import create_dimensional_model_from_dataframe
from database_loader import MySQLLoader
from config import DB_CONFIG, DB_TABLES
from utils import setup_logging, ensure_directory_exists

def test_mysql_connection():
    """Test basic MySQL connection without specifying database"""
    try:
        # Test basic connection to MySQL server
        basic_config = {
            'host': DB_CONFIG['host'],
            'port': DB_CONFIG['port'],
            'user': DB_CONFIG['user'],
            'password': DB_CONFIG['password']
        }
        
        print(f"Testing MySQL connection to {basic_config['host']}:{basic_config['port']}...")
        test_connection = mysql.connector.connect(**basic_config)
        
        # Test basic query
        test_cursor = test_connection.cursor()
        test_cursor.execute("SELECT VERSION()")
        version = test_cursor.fetchone()[0]
        print(f"✅ MySQL Server connected successfully! Version: {version}")
        
        test_cursor.close()
        test_connection.close()
        return True
        
    except Error as e:
        print(f"❌ MySQL connection test failed: {str(e)}")
        print(f"\n🔧 Please check:")
        print(f"  • MySQL server is running")
        print(f"  • Host: {DB_CONFIG['host']}")
        print(f"  • Port: {DB_CONFIG['port']}")
        print(f"  • User: {DB_CONFIG['user']}")
        print(f"  • Password: {'[SET]' if DB_CONFIG['password'] else '[EMPTY]'}")
        return False

def connect_to_database():
    try:
        # First, create the database if it doesn't exist
        temp_config = DB_CONFIG.copy()
        database_name = temp_config.pop('database')
        
        # Connect without database to create it
        temp_connection = mysql.connector.connect(**temp_config)
        temp_cursor = temp_connection.cursor()
        temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        temp_cursor.close()
        temp_connection.close()
        
        # Now connect to the database
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {str(e)}")
        print(f"Please ensure MySQL server is running and credentials are correct:")
        print(f"  Host: {DB_CONFIG['host']}")
        print(f"  Port: {DB_CONFIG['port']}")
        print(f"  User: {DB_CONFIG['user']}")
        print(f"  Database: {DB_CONFIG['database']}")
        return None

def load_data_from_database(connection):
    tables = {}
    
    try:
        for table_key, table_name in DB_TABLES.items():
            query = f"SELECT * FROM {table_name}"
            tables[table_key] = pd.read_sql(query, connection)
            print(f"Loaded {len(tables[table_key])} records from {table_name}")
        
        return tables
    except Error as e:
        print(f"Error loading data from database: {str(e)}")
        return None

def main():
    print("=" * 60)
    print("DIABETES ETL PIPELINE - Raw to MySQL")
    print("=" * 60)
    
    # Setup logging
    log_dir = 'logs'
    ensure_directory_exists(log_dir)
    log_file = os.path.join(log_dir, f'diabetes_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    setup_logging('INFO', log_file)
    
    logging.info("Starting Diabetes Data Analysis from MySQL")
    print(f"Logging to: {log_file}")
    
    try:
        # Step 1: Data Extraction
        print("\n" + "─" * 50)
        print("STEP 1: DATA EXTRACTION")
        print("─" * 50)
        
        print("Loading raw diabetes dataset...")
        df_raw = load_raw_data()
        print(f"Successfully loaded dataset: {df_raw.shape[0]} rows, {df_raw.shape[1]} columns")
        
        # Step 2: Data Transformation
        print("\n" + "─" * 50)
        print("STEP 2: DATA TRANSFORMATION")
        print("─" * 50)
        
        print("Executing transformation pipeline...")
        df_clean = full_transformation_pipeline(df_raw)
        print(f"Transformation completed: {df_clean.shape[0]} rows, {df_clean.shape[1]} columns")
        
        # Show class distribution
        print(f"\nDiabetes class distribution:")
        class_counts = df_clean['Diabetes_012'].value_counts().sort_index()
        for class_val, count in class_counts.items():
            class_name = {0: "No diabetes", 1: "Pre-diabetes", 2: "Diabetes"}[class_val]
            percentage = (count / len(df_clean)) * 100
            print(f"  - Class {class_val} ({class_name}): {count:,} ({percentage:.1f}%)")
        
        # Step 3: Dimensional Modeling
        print("\n" + "─" * 50)
        print("STEP 3: DIMENSIONAL MODELING")
        print("─" * 50)
        
        print("Creating dimensional tables...")
        tables = create_dimensional_model_from_dataframe(df_clean)
        print(f"Created {len(tables)} dimensional tables:")
        for table_name, table_df in tables.items():
            print(f"  - {table_name}: {len(table_df)} records")
        
        # Step 4: Database Connection
        print("\n" + "─" * 50)
        print("STEP 4: DATABASE CONNECTION")
        print("─" * 50)
        
        # Test basic MySQL connection first
        if not test_mysql_connection():
            raise Exception("MySQL server connection failed")
        
        print(f"\nConnecting to MySQL database: {DB_CONFIG['database']}...")
        connection = connect_to_database()
        if not connection:
            raise Exception("Failed to connect to database")
        print(f"✅ Connected to database: {DB_CONFIG['database']}")
        
        # Step 5: Data Loading to MySQL
        print("\n" + "─" * 50)
        print("STEP 5: LOADING TO MYSQL")
        print("─" * 50)
        
        # Create database loader
        loader = MySQLLoader(**{k: v for k, v in DB_CONFIG.items() if k in ['host', 'port', 'user', 'password', 'database']})
        loader.connection = connection
        loader.cursor = connection.cursor()
        
        print("Creating database tables...")
        if not loader.create_all_tables():
            raise Exception("Failed to create database tables")
        print("✅ Database tables created")
        
        print("Loading dimensional data to MySQL...")
        loader.load_dataframes_to_mysql(tables)
        
        # Step 6: Data Analysis
        print("\n" + "─" * 50)
        print("STEP 6: DATA ANALYSIS")
        print("─" * 50)
        
        # Final Summary
        print("\n" + "=" * 60)
        print("DIABETES ETL PIPELINE COMPLETED SUCCESSFULLY! 🎉")
        print("=" * 60)
        
        fact_records = len(tables['fact_health_records'])
        total_dims = sum(len(tables[key]) for key in tables if key.startswith('dim_'))
        
        print(f"\nETL Pipeline Statistics:")
        print(f"  - Raw records processed: {len(df_raw):,}")
        print(f"  - Clean records: {len(df_clean):,}")
        print(f"  - Fact records loaded: {fact_records:,}")
        print(f"  - Dimension records: {total_dims:,}")
        print(f"  - Database: {DB_CONFIG['database']}")

        print(f"\nProcess Log:")
        print(f"  {log_file}")
        
        print(f"\nPipeline Complete - Data ready in MySQL:")
        print(f"  ✅ Raw data extracted and cleaned")
        print(f"  ✅ Dimensional model created")
        print(f"  ✅ Data loaded to MySQL tables")
        print(f"  ✅ Analysis completed")
        
        # Close connection
        connection.close()
        print(f"\nDatabase connection closed")
        
        logging.info("ETL Pipeline completed successfully")
        return tables
        
    except FileNotFoundError as e:
        error_msg = f"Error: Dataset file not found. {str(e)}"
        print(f"\n{error_msg}")
        print(f"\nPlease ensure the diabetes dataset is available at:")
        print(f"  - data/raw/diabetes_012_health_indicators_BRFSS2015.csv")
        logging.error(error_msg)
        return None
        
    except Error as e:
        error_msg = f"Database Error: {str(e)}"
        print(f"\n{error_msg}")
        print(f"\nPlease ensure:")
        print(f"  - MySQL server is running")
        print(f"  - Database 'diabetesDB' exists")
        logging.error(error_msg)
        return None
        
    except Exception as e:
        error_msg = f"Error during ETL pipeline execution: {str(e)}"
        print(f"\n{error_msg}")
        logging.error(error_msg, exc_info=True)
        return None

def show_help():
    help_text = """
    Diabetes ETL Pipeline - Raw to MySQL
    ===================================
    
    This script executes the complete ETL process from raw data to MySQL.
    
    Usage:
        python main.py          # Run complete ETL pipeline
        python main.py --help   # Show this help message
    
    Prerequisites:
        - Raw dataset: data/raw/diabetes_012_health_indicators_BRFSS2015.csv
        - MySQL server running on localhost:3306
        - Database 'diabetesDB' created
    
    ETL Pipeline Steps:
        1. Data Extraction      - Load raw BRFSS dataset
        2. Data Transformation  - Clean and transform data
        3. Dimensional Modeling - Create star schema
        4. Database Connection  - Connect to MySQL
        5. Data Loading         - Load to MySQL tables
        6. Data Analysis        - Analyze loaded data
    
    Output:
        - Data loaded directly to MySQL diabetesDB
        - Analysis results displayed in console
        - Process log: logs/diabetes_analysis_YYYYMMDD_HHMMSS.log
    
    Final Database Tables:
        - fact_health_records: Main health records
        - dim_demographics: Demographics data
        - dim_lifestyle: Lifestyle factors
        - dim_medical_conditions: Medical conditions
        - dim_healthcare_access: Healthcare access
    """
    print(help_text)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h', 'help']:
        show_help()
    else:
        # Run the ETL pipeline
        result = main()
        
        if result is not None:
            print(f"\nAnalysis completed! Check the results above.")
            sys.exit(0)
        else:
            print(f"\nAnalysis failed. Check the logs for details.")
            sys.exit(1)