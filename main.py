import sys
import os
import mysql.connector
from mysql.connector import Error
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from extraction import load_raw_data
from transform import full_transformation_pipeline
from dimensional_etl import dimensional_model
from load import MySQLLoader
from config import DB_CONFIG

def test_mysql_connection():
    try:
        basic_config = {
            'host': DB_CONFIG['host'],
            'port': DB_CONFIG['port'],
            'user': DB_CONFIG['user'],
            'password': DB_CONFIG['password']
        }
        
        print(f"Testing MySQL connection to {basic_config['host']}:{basic_config['port']}...")
        test_connection = mysql.connector.connect(**basic_config)
        
        cursor = test_connection.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        print(f"MySQL Server connected successfully! Version: {version}")
        
        cursor.close()
        test_connection.close()
        return True
        
    except Error as e:
        print(f"MySQL connection test failed: {str(e)}")
        return False

def connect_to_database():
    try:
        temp_config = DB_CONFIG.copy()
        database_name = temp_config.pop('database')

        temp_connection = mysql.connector.connect(**temp_config)
        temp_cursor = temp_connection.cursor()
        temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        temp_cursor.close()
        temp_connection.close()
        
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {str(e)}")
        return None


def main():
    print("=" * 60)
    print("DIABETES ETL PIPELINE")
    print("=" * 60)

    try:
        print("\n[1] DATA EXTRACTION")
        df_raw = load_raw_data()
        print("Extraction completed")
        
        print("\n[2] DATA TRANSFORMATION")
        df_clean = full_transformation_pipeline(df_raw)
        print("Transformation completed")
        
        print("\nClass distribution:")
        class_counts = df_clean['Diabetes_012'].value_counts().sort_index()
        for class_val, count in class_counts.items():
            class_name = {0: "No diabetes", 1: "Pre-diabetes", 2: "Diabetes"}[class_val]
            percentage = (count / len(df_clean)) * 100
            print(f"  - {class_name}: {count:,} ({percentage:.1f}%)")
        
        print("\n[3] DIMENSIONAL MODELING")
        tables = dimensional_model(df_clean)
        print("Dimensional model created")

        print("\n[4] DATABASE CONNECTION")
        if not test_mysql_connection():
            raise Exception("MySQL server connection failed")
        
        connection = connect_to_database()
        if not connection:
            raise Exception("Failed to connect to database")
        print(f"✅ Connected to {DB_CONFIG['database']}")
        
        # Step 5: Loading
        print("\n[5] LOADING TO MYSQL")
        loader = MySQLLoader(**{k: v for k, v in DB_CONFIG.items() if k in ['host', 'port', 'user', 'password', 'database']})
        loader.connection = connection
        loader.cursor = connection.cursor()
        
        if not loader.create_all_tables():
            raise Exception("Failed to create database tables")
        
        loader.load_dataframes_to_mysql(tables)
        print("Loading completed")
        
        print("\n" + "=" * 60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        print("Processing completed successfully")
        
        connection.close()
        print("Database connection closed")
        return tables

    except Exception as e:
        print(f"\nError during ETL: {str(e)}")
        return None


if __name__ == "__main__":
    result = main()
    if result is not None:
        print("\nPipeline completed successfully.")
        sys.exit(0)
    else:
        print("\nPipeline failed.")
        sys.exit(1)