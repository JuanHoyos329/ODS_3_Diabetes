"""
Simplified Diabetes ETL DAG for Testing
Use this for quick testing without TaskGroups
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys
import os
import logging

# Add project path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(PROJECT_ROOT, 'src'))

from extraction import load_raw_data
from transform import full_transformation_pipeline
from dimensional_etl import dimensional_model
from load import MySQLLoader
from config import DB_CONFIG

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def extract_and_transform(**context):
    """Combined extract and transform for quick testing"""
    logging.info("Starting extraction and transformation...")
    
    # Extract
    df_raw = load_raw_data()
    logging.info(f"Extracted: {len(df_raw)} rows")
    
    # Transform
    df_clean = full_transformation_pipeline(df_raw)
    logging.info(f"Transformed: {len(df_clean)} rows")
    
    # Save temporarily
    import pandas as pd
    temp_path = os.path.join(PROJECT_ROOT, 'data', 'temp', 'clean_data.parquet')
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    df_clean.to_parquet(temp_path)
    
    context['ti'].xcom_push(key='clean_data_path', value=temp_path)
    context['ti'].xcom_push(key='row_count', value=len(df_clean))
    
    return temp_path


def model_and_load(**context):
    """Combined modeling and loading for quick testing"""
    import pandas as pd
    
    # Get clean data
    clean_path = context['ti'].xcom_pull(key='clean_data_path', task_ids='extract_and_transform')
    df_clean = pd.read_parquet(clean_path)
    
    logging.info("Creating dimensional model...")
    tables = dimensional_model(df_clean)
    
    logging.info("Loading to MySQL...")
    loader = MySQLLoader()
    if not loader.connect():
        raise Exception("Failed to connect to MySQL")
    
    loader.create_all_tables()
    loader.load_dataframes_to_mysql(tables)
    
    # Verify
    counts = loader.verify_data_load()
    logging.info(f"Load verification: {counts}")
    
    loader.disconnect()
    
    return counts


with DAG(
    dag_id='diabetes_etl_simple',
    default_args=default_args,
    description='Simplified ETL pipeline for testing',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'diabetes', 'test'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    extract_transform = PythonOperator(
        task_id='extract_and_transform',
        python_callable=extract_and_transform,
    )
    
    model_load = PythonOperator(
        task_id='model_and_load',
        python_callable=model_and_load,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> extract_transform >> model_load >> end
