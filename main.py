# Main ETL pipeline - loads raw data, transforms, and loads to MySQL

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

def connect_to_database():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"❌ Error connecting to MySQL: {str(e)}")
        return None

def load_data_from_database(connection):
    tables = {}
    
    try:
        for table_key, table_name in DB_TABLES.items():
            query = f"SELECT * FROM {table_name}"
            tables[table_key] = pd.read_sql(query, connection)
            print(f"✅ Loaded {len(tables[table_key])} records from {table_name}")
        
        return tables
    except Error as e:
        print(f"❌ Error loading data from database: {str(e)}")
        return None

def analyze_diabetes_data(tables):
    fact_df = tables['fact_health_records']
    
    # Set up plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Basic statistics
    print(f"\n📊 Dataset Statistics:")
    print(f"  - Total health records: {len(fact_df):,}")
    
    # Diabetes distribution
    diabetes_counts = fact_df['diabetes_status'].value_counts().sort_index()
    print(f"\n🩺 Diabetes Distribution:")
    class_names = {0: "No diabetes", 1: "Pre-diabetes", 2: "Diabetes"}
    for class_val, count in diabetes_counts.items():
        percentage = (count / len(fact_df)) * 100
        print(f"  - {class_names[class_val]}: {count:,} ({percentage:.1f}%)")
    
    # Create diabetes distribution pie chart
    plt.figure(figsize=(10, 6))
    labels = [class_names[i] for i in sorted(diabetes_counts.keys())]
    sizes = [diabetes_counts[i] for i in sorted(diabetes_counts.keys())]
    colors = ['#66b3ff', '#ffcc99', '#ff9999']
    
    plt.subplot(1, 2, 1)
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90)
    plt.title('Diabetes Status Distribution')
    
    # Create diabetes distribution bar chart
    plt.subplot(1, 2, 2)
    bars = plt.bar(labels, sizes, color=colors)
    plt.title('Diabetes Status Count')
    plt.ylabel('Number of Records')
    for i, bar in enumerate(bars):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1000, 
                f'{sizes[i]:,}', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig('diabetes_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✅ Diabetes distribution charts saved to diabetes_distribution.png")
    
    # Age group analysis if demographics available
    if 'dim_demographics' in tables:
        demo_df = tables['dim_demographics']
        # Join fact with demographics
        merged_df = fact_df.merge(demo_df, on='demographic_id', how='left')
        
        print(f"\n👥 Age Group Analysis:")
        age_diabetes = merged_df.groupby(['age_group', 'diabetes_status']).size().unstack(fill_value=0)
        diabetes_rates = []
        age_groups = []
        
        for age_group in sorted(merged_df['age_group'].unique()):
            if age_group in age_diabetes.index:
                total = age_diabetes.loc[age_group].sum()
                if 2.0 in age_diabetes.columns:
                    diabetes_rate = (age_diabetes.loc[age_group, 2.0] / total * 100) if total > 0 else 0
                    print(f"  - Age Group {age_group}: {diabetes_rate:.1f}% diabetes rate")
                    diabetes_rates.append(diabetes_rate)
                    age_groups.append(f"Group {int(age_group)}")
        
        # Create age group diabetes rate chart
        if diabetes_rates:
            plt.figure(figsize=(12, 6))
            bars = plt.bar(age_groups, diabetes_rates, color='#ff6b6b', alpha=0.7)
            plt.title('Diabetes Rate by Age Group')
            plt.xlabel('Age Group')
            plt.ylabel('Diabetes Rate (%)')
            plt.xticks(rotation=45)
            for i, bar in enumerate(bars):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                        f'{diabetes_rates[i]:.1f}%', ha='center', va='bottom')
            plt.tight_layout()
            plt.savefig('age_group_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
            print("✅ Age group analysis chart saved to age_group_analysis.png")
    
    # Lifestyle analysis
    if 'dim_lifestyle' in tables:
        lifestyle_df = tables['dim_lifestyle']
        lifestyle_merged = fact_df.merge(lifestyle_df, on='lifestyle_id', how='left')
        
        print(f"\n🏃 Lifestyle Impact Analysis:")
        # Physical Activity vs Diabetes
        if 'physical_activity' in lifestyle_merged.columns:
            phys_act = lifestyle_merged.groupby(['physical_activity', 'diabetes_status']).size().unstack(fill_value=0)
            activity_rates = []
            activity_labels = []
            
            for activity in sorted(lifestyle_merged['physical_activity'].unique()):
                if activity in phys_act.index:
                    total = phys_act.loc[activity].sum()
                    if 2.0 in phys_act.columns:
                        diabetes_rate = (phys_act.loc[activity, 2.0] / total * 100) if total > 0 else 0
                        activity_status = "Active" if activity == 1.0 else "Inactive"
                        print(f"  - {activity_status}: {diabetes_rate:.1f}% diabetes rate")
                        activity_rates.append(diabetes_rate)
                        activity_labels.append(activity_status)
            
            # Create lifestyle analysis charts
            if activity_rates and len(activity_rates) > 1:
                plt.figure(figsize=(15, 5))
                
                # Physical activity bar chart
                plt.subplot(1, 3, 1)
                bars = plt.bar(activity_labels, activity_rates, color=['#4CAF50', '#F44336'])
                plt.title('Diabetes Rate by Physical Activity')
                plt.ylabel('Diabetes Rate (%)')
                for i, bar in enumerate(bars):
                    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3, 
                            f'{activity_rates[i]:.1f}%', ha='center', va='bottom')
                
                # Smoking analysis
                if 'smoker_status' in lifestyle_merged.columns:
                    smoke_analysis = lifestyle_merged.groupby(['smoker_status', 'diabetes_status']).size().unstack(fill_value=0)
                    smoke_rates = []
                    smoke_labels = []
                    
                    for smoke_status in sorted(lifestyle_merged['smoker_status'].unique()):
                        if smoke_status in smoke_analysis.index:
                            total = smoke_analysis.loc[smoke_status].sum()
                            if 2.0 in smoke_analysis.columns:
                                diabetes_rate = (smoke_analysis.loc[smoke_status, 2.0] / total * 100) if total > 0 else 0
                                smoke_label = "Smoker" if smoke_status == 1.0 else "Non-smoker"
                                smoke_rates.append(diabetes_rate)
                                smoke_labels.append(smoke_label)
                    
                    if len(smoke_rates) > 1:
                        plt.subplot(1, 3, 2)
                        bars = plt.bar(smoke_labels, smoke_rates, color=['#FF9800', '#2196F3'])
                        plt.title('Diabetes Rate by Smoking Status')
                        plt.ylabel('Diabetes Rate (%)')
                        for i, bar in enumerate(bars):
                            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3, 
                                    f'{smoke_rates[i]:.1f}%', ha='center', va='bottom')
                
                # BMI distribution
                plt.subplot(1, 3, 3)
                diabetes_bmi = fact_df[fact_df['diabetes_status'] == 2]['bmi_value']
                no_diabetes_bmi = fact_df[fact_df['diabetes_status'] == 0]['bmi_value']
                
                plt.hist([no_diabetes_bmi, diabetes_bmi], bins=30, alpha=0.7, 
                        label=['No Diabetes', 'Diabetes'], color=['#4CAF50', '#F44336'])
                plt.title('BMI Distribution')
                plt.xlabel('BMI')
                plt.ylabel('Count')
                plt.legend()
                
                plt.tight_layout()
                plt.savefig('lifestyle_analysis.png', dpi=300, bbox_inches='tight')
                plt.close()
                print("✅ Lifestyle impact analysis charts saved to lifestyle_analysis.png")


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
        print(f"✅ Successfully loaded dataset: {df_raw.shape[0]} rows, {df_raw.shape[1]} columns")
        
        # Step 2: Data Transformation
        print("\n" + "─" * 50)
        print("STEP 2: DATA TRANSFORMATION")
        print("─" * 50)
        
        print("Executing transformation pipeline...")
        df_clean = full_transformation_pipeline(df_raw)
        print(f"✅ Transformation completed: {df_clean.shape[0]} rows, {df_clean.shape[1]} columns")
        
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
        print(f"✅ Created {len(tables)} dimensional tables:")
        for table_name, table_df in tables.items():
            print(f"  - {table_name}: {len(table_df)} records")
        
        # Step 4: Database Connection
        print("\n" + "─" * 50)
        print("STEP 4: DATABASE CONNECTION")
        print("─" * 50)
        
        print(f"Connecting to MySQL database: {DB_CONFIG['database']}...")
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
        if not loader.create_dimension_tables():
            raise Exception("Failed to create dimension tables")
        if not loader.create_fact_table():
            raise Exception("Failed to create fact table")
        print("✅ Database tables created")
        
        print("Loading dimensional data to MySQL...")
        loader.load_dataframes_to_mysql(tables)
        
        # Step 6: Data Analysis
        print("\n" + "─" * 50)
        print("STEP 6: DATA ANALYSIS")
        print("─" * 50)
        
        print("Analyzing loaded data...")
        analyze_diabetes_data(tables)
        print("✅ Analysis completed")
        
        # Final Summary
        print("\n" + "=" * 60)
        print("DIABETES ETL PIPELINE COMPLETED SUCCESSFULLY! 🎉")
        print("=" * 60)
        
        fact_records = len(tables['fact_health_records'])
        total_dims = sum(len(tables[key]) for key in tables if key.startswith('dim_'))
        
        print(f"\n📊 ETL Pipeline Statistics:")
        print(f"  - Raw records processed: {len(df_raw):,}")
        print(f"  - Clean records: {len(df_clean):,}")
        print(f"  - Fact records loaded: {fact_records:,}")
        print(f"  - Dimension records: {total_dims:,}")
        print(f"  - Database: {DB_CONFIG['database']}")
        
        print(f"\n📁 Process Log:")
        print(f"  📁 {log_file}")
        
        print(f"\n🚀 Pipeline Complete - Data ready in MySQL:")
        print(f"  ✅ Raw data extracted and cleaned")
        print(f"  ✅ Dimensional model created")
        print(f"  ✅ Data loaded to MySQL tables")
        print(f"  ✅ Analysis completed")
        
        # Close connection
        connection.close()
        print(f"\n🔌 Database connection closed")
        
        logging.info("ETL Pipeline completed successfully")
        return tables
        
    except FileNotFoundError as e:
        error_msg = f"❌ Error: Dataset file not found. {str(e)}"
        print(f"\n{error_msg}")
        print(f"\nPlease ensure the diabetes dataset is available at:")
        print(f"  - data/raw/diabetes_012_health_indicators_BRFSS2015.csv")
        logging.error(error_msg)
        return None
        
    except Error as e:
        error_msg = f"❌ Database Error: {str(e)}"
        print(f"\n{error_msg}")
        print(f"\nPlease ensure:")
        print(f"  - MySQL server is running")
        print(f"  - Database 'diabetesDB' exists")
        logging.error(error_msg)
        return None
        
    except Exception as e:
        error_msg = f"❌ Error during ETL pipeline execution: {str(e)}"
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
            print(f"\n🚀 Analysis completed! Check the results above.")
            sys.exit(0)
        else:
            print(f"\n💥 Analysis failed. Check the logs for details.")
            sys.exit(1)