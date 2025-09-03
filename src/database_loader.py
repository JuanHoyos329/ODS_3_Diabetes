# MySQL loader for diabetes dimensional tables

import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from utils import setup_logging, ensure_directory_exists
import sys
sys.path.append('..')
from config import DB_CONFIG, DB_TABLES, DB_VIEWS

class MySQLLoader:
    
    def __init__(self, host: str = 'localhost', port: int = 3306, 
                 user: str = 'root', password: str = '', database: str = 'diabetes_dw'):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'raise_on_warnings': True
        }
        self.connection = None
        self.cursor = None
        
    def connect(self) -> bool:
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            logging.info(f"Connected to MySQL database: {self.config['database']}")
            print(f"✅ Connected to MySQL database: {self.config['database']}")
            return True
        except Error as e:
            logging.error(f"Error connecting to MySQL: {str(e)}")
            print(f"❌ Error connecting to MySQL: {str(e)}")
            return False
    
    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("MySQL connection closed")
            print("✅ MySQL connection closed")
    
    def load_dataframes_to_mysql(self, tables_dict):
        for table_key, table_df in tables_dict.items():
            table_name = DB_TABLES[table_key]
            # Clear existing data
            self.cursor.execute(f"DELETE FROM {table_name}")
            # Load new data
            self._load_dataframe_to_table(table_df, table_name)
        
        self.connection.commit()
        logging.info("All DataFrames loaded to MySQL successfully")
        print("✅ All data loaded to MySQL")
    
    def _load_dataframe_to_table(self, df, table_name):
        # Create INSERT statement
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Clean NaN values and convert to list of tuples
        df_clean = df.fillna(0)
        data_tuples = [tuple(row) for row in df_clean.values]
        
        # Execute batch insert
        self.cursor.executemany(insert_sql, data_tuples)
        
        print(f"✅ Loaded {len(df)} records into {table_name}")
    
    def create_database(self, database_name: str = None) -> bool:
        if database_name is None:
            database_name = self.config['database']
            
        try:
            # Connect without specifying database
            temp_config = self.config.copy()
            del temp_config['database']
            
            temp_connection = mysql.connector.connect(**temp_config)
            temp_cursor = temp_connection.cursor()
            
            # Create database
            temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            temp_cursor.execute(f"USE {database_name}")
            
            temp_cursor.close()
            temp_connection.close()
            
            logging.info(f"Database {database_name} created/verified")
            print(f"✅ Database {database_name} created/verified")
            return True
            
        except Error as e:
            # Error 1007 means database already exists, which is OK
            if e.errno == 1007:
                logging.info(f"Database {database_name} already exists")
                print(f"✅ Database {database_name} already exists")
                return True
            else:
                logging.error(f"Error creating database: {str(e)}")
                print(f"❌ Error creating database: {str(e)}")
                return False
    
    def create_dimension_tables(self) -> bool:
        try:
            # Drop tables if they exist (for clean reload)
            drop_statements = [
                "DROP TABLE IF EXISTS fact_health_records",
                "DROP TABLE IF EXISTS dim_demographics", 
                "DROP TABLE IF EXISTS dim_lifestyle",
                "DROP TABLE IF EXISTS dim_medical_conditions",
                "DROP TABLE IF EXISTS dim_healthcare_access"
            ]
            
            for statement in drop_statements:
                self.cursor.execute(statement)
                
            # Create dimension tables
            create_statements = {
                "dim_demographics": """
                    CREATE TABLE dim_demographics (
                        demographic_id INT PRIMARY KEY,
                        sex DECIMAL(3,1) NOT NULL COMMENT '0=Female, 1=Male',
                        age_group DECIMAL(3,1) NOT NULL COMMENT '1-13 representing 5-year increments',
                        education_level DECIMAL(3,1) NOT NULL COMMENT '1-6 education levels',
                        income_bracket DECIMAL(3,1) NOT NULL COMMENT '1-8 income brackets',
                        INDEX idx_sex (sex),
                        INDEX idx_age_group (age_group),
                        INDEX idx_education (education_level),
                        INDEX idx_income (income_bracket)
                    ) ENGINE=InnoDB COMMENT='Demographics dimension table'
                """,
                
                "dim_lifestyle": """
                    CREATE TABLE dim_lifestyle (
                        lifestyle_id INT PRIMARY KEY,
                        smoker_status DECIMAL(3,1) NOT NULL COMMENT '0=Non-smoker, 1=Smoker',
                        physical_activity DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes',
                        fruits_consumption DECIMAL(3,1) NOT NULL COMMENT '0=<1/day, 1=1+/day',
                        vegetables_consumption DECIMAL(3,1) NOT NULL COMMENT '0=<1/day, 1=1+/day',
                        heavy_alcohol_consumption DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes',
                        INDEX idx_smoker (smoker_status),
                        INDEX idx_physical_activity (physical_activity)
                    ) ENGINE=InnoDB COMMENT='Lifestyle dimension table'
                """,
                
                "dim_medical_conditions": """
                    CREATE TABLE dim_medical_conditions (
                        medical_conditions_id INT PRIMARY KEY,
                        high_blood_pressure DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes',
                        high_cholesterol DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes', 
                        cholesterol_check DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes',
                        stroke_history DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes',
                        heart_disease_or_attack DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes',
                        difficulty_walking DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes',
                        INDEX idx_high_bp (high_blood_pressure),
                        INDEX idx_high_chol (high_cholesterol),
                        INDEX idx_heart_disease (heart_disease_or_attack)
                    ) ENGINE=InnoDB COMMENT='Medical conditions dimension table'
                """,
                
                "dim_healthcare_access": """
                    CREATE TABLE dim_healthcare_access (
                        healthcare_access_id INT PRIMARY KEY,
                        any_healthcare_coverage DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes',
                        no_doctor_due_to_cost DECIMAL(3,1) NOT NULL COMMENT '0=No, 1=Yes',
                        INDEX idx_healthcare_coverage (any_healthcare_coverage),
                        INDEX idx_doctor_cost (no_doctor_due_to_cost)
                    ) ENGINE=InnoDB COMMENT='Healthcare access dimension table'
                """
            }
            
            for table_name, create_sql in create_statements.items():
                self.cursor.execute(create_sql)
                logging.info(f"Created table: {table_name}")
                print(f"✅ Created table: {table_name}")
            
            self.connection.commit()
            return True
            
        except Error as e:
            logging.error(f"Error creating dimension tables: {str(e)}")
            print(f"❌ Error creating dimension tables: {str(e)}")
            return False
    
    def create_fact_table(self) -> bool:
        try:
            create_fact_sql = """
                CREATE TABLE fact_health_records (
                    record_id BIGINT PRIMARY KEY,
                    diabetes_status DECIMAL(3,1) NOT NULL COMMENT '0=No diabetes, 1=Pre-diabetes, 2=Diabetes',
                    bmi_value DECIMAL(5,2) NOT NULL COMMENT 'Body Mass Index',
                    mental_health_days DECIMAL(3,1) NOT NULL COMMENT 'Days 0-30',
                    physical_health_days DECIMAL(3,1) NOT NULL COMMENT 'Days 0-30', 
                    general_health_score DECIMAL(3,1) NOT NULL COMMENT '1=Excellent to 5=Poor',
                    demographic_id INT NOT NULL,
                    lifestyle_id INT NOT NULL,
                    medical_conditions_id INT NOT NULL,
                    healthcare_access_id INT NOT NULL,
                    
                    FOREIGN KEY (demographic_id) REFERENCES dim_demographics(demographic_id),
                    FOREIGN KEY (lifestyle_id) REFERENCES dim_lifestyle(lifestyle_id),
                    FOREIGN KEY (medical_conditions_id) REFERENCES dim_medical_conditions(medical_conditions_id),
                    FOREIGN KEY (healthcare_access_id) REFERENCES dim_healthcare_access(healthcare_access_id),
                    
                    INDEX idx_diabetes_status (diabetes_status),
                    INDEX idx_bmi (bmi_value),
                    INDEX idx_demographic (demographic_id),
                    INDEX idx_lifestyle (lifestyle_id),
                    INDEX idx_medical (medical_conditions_id),
                    INDEX idx_healthcare (healthcare_access_id),
                    INDEX idx_diabetes_bmi (diabetes_status, bmi_value)
                ) ENGINE=InnoDB COMMENT='Health records fact table'
            """
            
            self.cursor.execute(create_fact_sql)
            self.connection.commit()
            
            logging.info("Created fact table: fact_health_records")
            print("✅ Created fact table: fact_health_records")
            return True
            
        except Error as e:
            logging.error(f"Error creating fact table: {str(e)}")
            print(f"❌ Error creating fact table: {str(e)}")
            return False
    
    def load_csv_to_table(self, csv_file: str, table_name: str) -> bool:
        try:
            if not os.path.exists(csv_file):
                raise FileNotFoundError(f"CSV file not found: {csv_file}")
            
            # Read CSV
            df = pd.read_csv(csv_file)
            logging.info(f"Loaded CSV {csv_file}: {len(df)} rows")
            
            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df.values]
            
            # Create INSERT statement
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Execute batch insert
            self.cursor.executemany(insert_sql, data)
            self.connection.commit()
            
            logging.info(f"Loaded {len(data)} records into {table_name}")
            print(f"✅ Loaded {len(data):,} records into {table_name}")
            return True
            
        except Error as e:
            logging.error(f"Error loading data to {table_name}: {str(e)}")
            print(f"❌ Error loading data to {table_name}: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"General error loading {csv_file}: {str(e)}")
            print(f"❌ General error loading {csv_file}: {str(e)}")
            return False
    
    def verify_data_load(self) -> Dict[str, int]:
        tables = [
            'dim_demographics',
            'dim_lifestyle', 
            'dim_medical_conditions',
            'dim_healthcare_access',
            'fact_health_records'
        ]
        
        counts = {}
        
        try:
            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                counts[table] = count
                logging.info(f"Table {table}: {count} records")
            
            return counts
            
        except Error as e:
            logging.error(f"Error verifying data load: {str(e)}")
            return {}
    
    def create_analysis_views(self) -> bool:
        try:
            views = {
                "vw_diabetes_summary": """
                    CREATE OR REPLACE VIEW vw_diabetes_summary AS
                    SELECT 
                        f.diabetes_status,
                        CASE 
                            WHEN f.diabetes_status = 0 THEN 'No Diabetes'
                            WHEN f.diabetes_status = 1 THEN 'Pre-diabetes'  
                            WHEN f.diabetes_status = 2 THEN 'Diabetes'
                        END as diabetes_label,
                        COUNT(*) as record_count,
                        AVG(f.bmi_value) as avg_bmi,
                        AVG(f.mental_health_days) as avg_mental_health_days,
                        AVG(f.physical_health_days) as avg_physical_health_days,
                        AVG(f.general_health_score) as avg_general_health_score
                    FROM fact_health_records f
                    GROUP BY f.diabetes_status
                    ORDER BY f.diabetes_status
                """,
                
                "vw_demographics_analysis": """
                    CREATE OR REPLACE VIEW vw_demographics_analysis AS
                    SELECT 
                        CASE WHEN d.sex = 0 THEN 'Female' ELSE 'Male' END as gender,
                        d.age_group,
                        d.education_level,
                        d.income_bracket,
                        f.diabetes_status,
                        COUNT(*) as record_count,
                        AVG(f.bmi_value) as avg_bmi
                    FROM fact_health_records f
                    JOIN dim_demographics d ON f.demographic_id = d.demographic_id  
                    GROUP BY d.sex, d.age_group, d.education_level, d.income_bracket, f.diabetes_status
                """,
                
                "vw_lifestyle_impact": """
                    CREATE OR REPLACE VIEW vw_lifestyle_impact AS
                    SELECT 
                        CASE WHEN l.smoker_status = 1 THEN 'Smoker' ELSE 'Non-smoker' END as smoking_status,
                        CASE WHEN l.physical_activity = 1 THEN 'Active' ELSE 'Inactive' END as activity_level,
                        f.diabetes_status,
                        COUNT(*) as record_count,
                        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY l.smoker_status, l.physical_activity) as percentage
                    FROM fact_health_records f
                    JOIN dim_lifestyle l ON f.lifestyle_id = l.lifestyle_id
                    GROUP BY l.smoker_status, l.physical_activity, f.diabetes_status
                    ORDER BY l.smoker_status, l.physical_activity, f.diabetes_status
                """
            }
            
            for view_name, view_sql in views.items():
                self.cursor.execute(view_sql)
                logging.info(f"Created view: {view_name}")
                print(f"✅ Created view: {view_name}")
            
            self.connection.commit()
            return True
            
        except Error as e:
            logging.error(f"Error creating views: {str(e)}")
            print(f"❌ Error creating views: {str(e)}")
            return False

def get_db_config() -> Dict[str, Any]:
    print("\n" + "─" * 50) 
    print("MySQL DATABASE CONFIGURATION")
    print("─" * 50)
    print("Using configuration from config.py")
    print(f"Host: {DB_CONFIG['host']}")
    print(f"Port: {DB_CONFIG['port']}")
    print(f"User: {DB_CONFIG['user']}")
    print(f"Database: {DB_CONFIG['database']}")
    
    # Check if password is configured
    config = DB_CONFIG.copy()
    if not DB_CONFIG['password']:
        print("\nNo password configured (empty password)")
        print("Password: [No password]")
    else:
        print("Password: [Configured in config.py]")
    
    # In non-interactive mode, use config as-is
    # Interactive prompts removed for automated execution
    
    return config

def show_help():
    help_text = """
    MySQL Database Loader for Diabetes Health Indicators
    ===================================================
    
    This script loads the dimensional star schema into a MySQL database.
    
    Usage:
        python3 database_loader.py          # Interactive mode
        python3 database_loader.py --help   # Show this help message
    
    Prerequisites:
        - MySQL Server 8.0+ installed and running
        - MySQL user with CREATE DATABASE privileges  
        - Dimensional CSV files in data/processed/
        - mysql-connector-python installed
    
    What it does:
        1. Creates database and tables with proper schema
        2. Loads all dimension tables (demographics, lifestyle, etc.)
        3. Loads fact table with foreign key constraints
        4. Creates indexes for query optimization
        5. Creates analysis views for common queries
        6. Verifies data load success
    
    Database created:
        - Database: diabetes_dw (configurable)
        - Tables: 4 dimensions + 1 fact table
        - Views: 3 analysis views
        - Records: 253,680 health records
    
    Example queries after loading:
        SELECT * FROM vw_diabetes_summary;
        SELECT * FROM vw_demographics_analysis LIMIT 10;
        SELECT * FROM vw_lifestyle_impact;
    """
    print(help_text)

def main():
    print("=" * 60)
    print("MYSQL DATABASE LOADER - DIABETES STAR SCHEMA")
    print("=" * 60)
    
    # Setup logging
    log_dir = 'logs'
    ensure_directory_exists(log_dir)
    log_file = os.path.join(log_dir, f'database_loader_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    setup_logging('INFO', log_file)
    
    logging.info("Starting Database Loader")
    print(f"Logging to: {log_file}")
    
    try:
        # Get database configuration
        db_config = get_db_config()
        
        # Initialize MySQL loader (filter out non-constructor parameters)
        loader_config = {k: v for k, v in db_config.items() 
                        if k in ['host', 'port', 'user', 'password', 'database']}
        loader = MySQLLoader(**loader_config)
        
        # Step 1: Create database and connect
        print("\n" + "─" * 50)
        print("STEP 1: DATABASE CONNECTION")
        print("─" * 50)
        
        if not loader.create_database():
            raise Exception("Failed to create/verify database")
        
        if not loader.connect():
            raise Exception("Failed to connect to database")
        
        # Step 2: Create tables
        print("\n" + "─" * 50)
        print("STEP 2: CREATING TABLES")
        print("─" * 50)
        
        print("Creating dimension tables...")
        if not loader.create_dimension_tables():
            raise Exception("Failed to create dimension tables")
        
        print("Creating fact table...")
        if not loader.create_fact_table():
            raise Exception("Failed to create fact table")
        
        # Step 3: Load data
        print("\n" + "─" * 50)
        print("STEP 3: LOADING DATA")
        print("─" * 50)
        
        data_files = {
            'dim_demographics': '../data/processed/dim_demographics.csv',
            'dim_lifestyle': '../data/processed/dim_lifestyle.csv',
            'dim_medical_conditions': '../data/processed/dim_medical_conditions.csv',
            'dim_healthcare_access': '../data/processed/dim_healthcare_access.csv',
            'fact_health_records': '../data/processed/fact_health_records.csv'
        }
        
        success_count = 0
        for table_name, csv_file in data_files.items():
            print(f"Loading {table_name}...")
            if loader.load_csv_to_table(csv_file, table_name):
                success_count += 1
            else:
                print(f"⚠️  Failed to load {table_name}")
        
        if success_count != len(data_files):
            raise Exception(f"Only {success_count}/{len(data_files)} tables loaded successfully")
        
        # Step 4: Verify data load
        print("\n" + "─" * 50)
        print("STEP 4: DATA VERIFICATION")
        print("─" * 50)
        
        print("Verifying data load...")
        counts = loader.verify_data_load()
        
        if counts:
            print("📊 Record counts:")
            total_records = 0
            for table, count in counts.items():
                print(f"  {table}: {count:,} records")
                total_records += count
            print(f"  Total: {total_records:,} records")
        
        # Step 5: Create analysis views
        print("\n" + "─" * 50)
        print("STEP 5: CREATING ANALYSIS VIEWS")
        print("─" * 50)
        
        print("Creating analysis views...")
        if loader.create_analysis_views():
            print("✅ Analysis views created successfully")
        else:
            print("⚠️  Some views may not have been created")
        
        # Close connection
        loader.disconnect()
        
        # Success summary
        print("\n" + "=" * 60)
        print("DATABASE LOAD COMPLETED SUCCESSFULLY! 🎉")
        print("=" * 60)
        
        print(f"\n🗄️  Database: {db_config['database']}")
        print(f"📊 Tables loaded: {len(data_files)}")
        print(f"🔍 Analysis views: 3")
        print(f"📈 Total records: {sum(counts.values()) if counts else 'Unknown'}")
        
        print(f"\n💡 Sample queries to try:")
        print(f"  SELECT * FROM vw_diabetes_summary;")
        print(f"  SELECT * FROM vw_demographics_analysis LIMIT 10;")
        print(f"  SELECT * FROM vw_lifestyle_impact LIMIT 10;")
        
        print(f"\n🌟 Your diabetes data warehouse is ready for analysis!")
        
        logging.info("Database loader completed successfully")
        return True
        
    except Exception as e:
        error_msg = f"❌ Error during database loading: {str(e)}"
        print(f"\n{error_msg}")
        logging.error(error_msg, exc_info=True)
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h', 'help']:
        show_help()
        sys.exit(0)
    else:
        success = main()
        sys.exit(0 if success else 1)