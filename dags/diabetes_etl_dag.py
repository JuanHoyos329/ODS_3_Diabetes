"""
Diabetes ETL Pipeline - Apache Airflow DAG
Author: Healthcare Data Team
Description: Orchestrates the ETL pipeline for diabetes health indicators from BRFSS data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import sys
import os
import logging
import mysql.connector
from mysql.connector import Error

# Add project path to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(PROJECT_ROOT, 'src'))

from extraction import load_raw_data
from transform import full_transformation_pipeline
from dimensional_etl import dimensional_model
from load import MySQLLoader
from config import DB_CONFIG

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


def test_mysql_connection(**context):
    """Task 1: Test MySQL database connection"""
    try:
        basic_config = {
            'host': DB_CONFIG['host'],
            'port': DB_CONFIG['port'],
            'user': DB_CONFIG['user'],
            'password': DB_CONFIG['password']
        }
        
        logging.info(f"Testing MySQL connection to {basic_config['host']}:{basic_config['port']}...")
        test_connection = mysql.connector.connect(**basic_config)
        
        cursor = test_connection.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        logging.info(f"MySQL Server connected successfully! Version: {version}")
        
        cursor.close()
        test_connection.close()
        
        # Push connection status to XCom
        context['ti'].xcom_push(key='mysql_connection_status', value='success')
        return True
        
    except Error as e:
        logging.error(f"MySQL connection test failed: {str(e)}")
        raise Exception(f"MySQL connection failed: {str(e)}")


def create_database(**context):
    """Task 2: Create database if not exists"""
    try:
        temp_config = DB_CONFIG.copy()
        database_name = temp_config.pop('database')

        temp_connection = mysql.connector.connect(**temp_config)
        temp_cursor = temp_connection.cursor()
        temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        logging.info(f"Database '{database_name}' created or already exists")
        temp_cursor.close()
        temp_connection.close()
        
        context['ti'].xcom_push(key='database_name', value=database_name)
        return database_name
        
    except Error as e:
        logging.error(f"Error creating database: {str(e)}")
        raise


def extract_raw_data(**context):
    """Task 3: Extract raw data from CSV files"""
    try:
        logging.info("Starting data extraction...")
        df_raw = load_raw_data()
        
        # Save to temporary location for next task
        temp_path = os.path.join(PROJECT_ROOT, 'data', 'temp', 'raw_extracted.parquet')
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        df_raw.to_parquet(temp_path)
        
        # Push metadata to XCom
        context['ti'].xcom_push(key='raw_data_path', value=temp_path)
        context['ti'].xcom_push(key='raw_data_rows', value=len(df_raw))
        context['ti'].xcom_push(key='raw_data_cols', value=len(df_raw.columns))
        
        logging.info(f"Extraction completed: {len(df_raw)} rows, {len(df_raw.columns)} columns")
        return temp_path
        
    except Exception as e:
        logging.error(f"Error during data extraction: {str(e)}")
        raise


def transform_data(**context):
    """Task 4: Transform and clean the data"""
    try:
        import pandas as pd
        
        # Get raw data path from previous task
        raw_data_path = context['ti'].xcom_pull(key='raw_data_path', task_ids='extract_raw_data')
        
        logging.info("Starting data transformation...")
        df_raw = pd.read_parquet(raw_data_path)
        df_clean = full_transformation_pipeline(df_raw)
        
        # Save cleaned data
        clean_path = os.path.join(PROJECT_ROOT, 'data', 'temp', 'cleaned_data.parquet')
        df_clean.to_parquet(clean_path)
        
        # Calculate class distribution
        class_counts = df_clean['diabetes_status'].value_counts().to_dict()
        
        # Push metadata to XCom
        context['ti'].xcom_push(key='clean_data_path', value=clean_path)
        context['ti'].xcom_push(key='clean_data_rows', value=len(df_clean))
        context['ti'].xcom_push(key='class_distribution', value=class_counts)
        
        logging.info(f"Transformation completed: {len(df_clean)} rows")
        logging.info(f"Class distribution: {class_counts}")
        return clean_path
        
    except Exception as e:
        logging.error(f"Error during data transformation: {str(e)}")
        raise


def create_dimensional_model(**context):
    """Task 5: Create dimensional model (star schema)"""
    try:
        import pandas as pd
        
        # Get cleaned data path from previous task
        clean_data_path = context['ti'].xcom_pull(key='clean_data_path', task_ids='transform_data')
        
        logging.info("Starting dimensional modeling...")
        df_clean = pd.read_parquet(clean_data_path)
        tables = dimensional_model(df_clean)
        
        # Save dimensional tables
        dim_path = os.path.join(PROJECT_ROOT, 'data', 'temp', 'dimensional')
        os.makedirs(dim_path, exist_ok=True)
        
        for table_name, df_table in tables.items():
            table_file = os.path.join(dim_path, f"{table_name}.parquet")
            df_table.to_parquet(table_file)
            logging.info(f"Saved {table_name}: {len(df_table)} rows")
        
        # Push metadata to XCom
        context['ti'].xcom_push(key='dimensional_path', value=dim_path)
        context['ti'].xcom_push(key='table_names', value=list(tables.keys()))
        
        logging.info("Dimensional modeling completed")
        return dim_path
        
    except Exception as e:
        logging.error(f"Error during dimensional modeling: {str(e)}")
        raise


def create_database_tables(**context):
    """Task 6: Create database schema and tables"""
    try:
        logging.info("Creating database tables...")
        
        loader = MySQLLoader()
        if not loader.connect():
            raise Exception("Failed to connect to MySQL database")
        
        if not loader.create_all_tables():
            raise Exception("Failed to create database tables")
        
        loader.disconnect()
        logging.info("Database tables created successfully")
        
        context['ti'].xcom_push(key='tables_created', value=True)
        return True
        
    except Exception as e:
        logging.error(f"Error creating database tables: {str(e)}")
        raise


def load_dimensional_tables(**context):
    """Task 7: Load dimensional tables to MySQL"""
    try:
        import pandas as pd
        
        # Get dimensional data path from previous task
        dim_path = context['ti'].xcom_pull(key='dimensional_path', task_ids='create_dimensional_model')
        table_names = context['ti'].xcom_pull(key='table_names', task_ids='create_dimensional_model')
        
        logging.info("Loading dimensional tables to MySQL...")
        
        loader = MySQLLoader()
        if not loader.connect():
            raise Exception("Failed to connect to MySQL database")
        
        # Load tables in correct order (dimensions first, then fact)
        dimension_tables = [t for t in table_names if t.startswith('dim_')]
        fact_tables = [t for t in table_names if t.startswith('fact_')]
        
        tables = {}
        for table_name in dimension_tables + fact_tables:
            table_file = os.path.join(dim_path, f"{table_name}.parquet")
            df_table = pd.read_parquet(table_file)
            tables[table_name] = df_table
        
        loader.load_dataframes_to_mysql(tables)
        loader.disconnect()
        
        logging.info("All tables loaded successfully")
        
        context['ti'].xcom_push(key='tables_loaded', value=True)
        return True
        
    except Exception as e:
        logging.error(f"Error loading tables: {str(e)}")
        raise


def verify_data_load(**context):
    """Task 8: Verify data was loaded correctly"""
    try:
        logging.info("Verifying data load...")
        
        loader = MySQLLoader()
        if not loader.connect():
            raise Exception("Failed to connect to MySQL database")
        
        counts = loader.verify_data_load()
        loader.disconnect()
        
        # Push verification results to XCom
        context['ti'].xcom_push(key='table_counts', value=counts)
        
        logging.info("Data verification completed")
        logging.info(f"Table counts: {counts}")
        return counts
        
    except Exception as e:
        logging.error(f"Error verifying data load: {str(e)}")
        raise


def cleanup_temp_files(**context):
    """Task 9: Clean up temporary files"""
    try:
        import shutil
        
        temp_dir = os.path.join(PROJECT_ROOT, 'data', 'temp')
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logging.info("Temporary files cleaned up")
        else:
            logging.info("No temporary files to clean up")
        
        return True
        
    except Exception as e:
        logging.warning(f"Error cleaning up temporary files: {str(e)}")
        # Don't raise exception - cleanup is not critical
        return False


def send_success_notification(**context):
    """Task 10: Send success notification with pipeline summary"""
    try:
        # Gather all metrics from XCom
        raw_rows = context['ti'].xcom_pull(key='raw_data_rows', task_ids='extract_raw_data')
        clean_rows = context['ti'].xcom_pull(key='clean_data_rows', task_ids='transform_data')
        class_dist = context['ti'].xcom_pull(key='class_distribution', task_ids='transform_data')
        table_counts = context['ti'].xcom_pull(key='table_counts', task_ids='verify_data_load')
        
        summary = f"""
        ================================================
        DIABETES ETL PIPELINE - SUCCESS
        ================================================
        Execution Date: {context['ds']}
        
        Data Processing:
        - Raw data extracted: {raw_rows:,} rows
        - Cleaned data: {clean_rows:,} rows
        - Data reduction: {raw_rows - clean_rows:,} rows removed
        
        Class Distribution:
        {chr(10).join([f'  - {k}: {v:,}' for k, v in (class_dist or {}).items()])}
        
        Database Load:
        {chr(10).join([f'  - {k}: {v:,} rows' for k, v in (table_counts or {}).items()])}
        
        Status: COMPLETED SUCCESSFULLY
        ================================================
        """
        
        logging.info(summary)
        print(summary)
        
        return True
        
    except Exception as e:
        logging.warning(f"Error sending notification: {str(e)}")
        return False


# Define the DAG
with DAG(
    dag_id='diabetes_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for diabetes health indicators from BRFSS data',
    schedule_interval='@weekly',  # Run weekly, adjust as needed
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'diabetes', 'health', 'brfss'],
    max_active_runs=1,
) as dag:

    # Start task
    start = EmptyOperator(
        task_id='start',
        doc_md="Pipeline start marker"
    )

    # Database preparation tasks
    with TaskGroup(group_id='database_setup') as database_setup:
        test_connection = PythonOperator(
            task_id='test_mysql_connection',
            python_callable=test_mysql_connection,
            doc_md="Test MySQL database connection"
        )

        create_db = PythonOperator(
            task_id='create_database',
            python_callable=create_database,
            doc_md="Create database if not exists"
        )

        test_connection >> create_db

    # ETL tasks
    with TaskGroup(group_id='etl_process') as etl_process:
        extract = PythonOperator(
            task_id='extract_raw_data',
            python_callable=extract_raw_data,
            doc_md="Extract raw data from CSV files"
        )

        transform = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data,
            doc_md="Transform and clean the data"
        )

        model = PythonOperator(
            task_id='create_dimensional_model',
            python_callable=create_dimensional_model,
            doc_md="Create dimensional model (star schema)"
        )

        extract >> transform >> model

    # Database loading tasks
    with TaskGroup(group_id='database_load') as database_load:
        create_tables = PythonOperator(
            task_id='create_database_tables',
            python_callable=create_database_tables,
            doc_md="Create database schema and tables"
        )

        load_tables = PythonOperator(
            task_id='load_dimensional_tables',
            python_callable=load_dimensional_tables,
            doc_md="Load dimensional tables to MySQL"
        )

        verify = PythonOperator(
            task_id='verify_data_load',
            python_callable=verify_data_load,
            doc_md="Verify data was loaded correctly"
        )

        create_tables >> load_tables >> verify

    # Cleanup and notification
    cleanup = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_temp_files,
        trigger_rule='all_done',  # Run even if previous tasks failed
        doc_md="Clean up temporary files"
    )

    notify = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        trigger_rule='all_success',
        doc_md="Send success notification with pipeline summary"
    )

    # End task
    end = EmptyOperator(
        task_id='end',
        trigger_rule='all_done',
        doc_md="Pipeline end marker"
    )

    # Define task dependencies
    start >> database_setup >> etl_process >> database_load >> cleanup >> notify >> end
