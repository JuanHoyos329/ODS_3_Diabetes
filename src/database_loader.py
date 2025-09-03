import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import sys
import logging
from datetime import datetime

from utils import setup_logging, ensure_directory_exists
sys.path.append('..')
from config import DB_CONFIG, DB_TABLES


def get_connection():
    """Get database connection using configuration from config.py"""
    return mysql.connector.connect(**DB_CONFIG)


def create_tables():
    """Create all tables in the database"""
    try:
        temp_config = DB_CONFIG.copy()
        database_name = temp_config.pop('database')

        temp_connection = mysql.connector.connect(**temp_config)
        temp_cursor = temp_connection.cursor()
        temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        temp_cursor.close()
        temp_connection.close()

        connection = get_connection()
        cursor = connection.cursor()

        drop_statements = [
            "DROP TABLE IF EXISTS fact_health_records",
            "DROP TABLE IF EXISTS dim_demographics",
            "DROP TABLE IF EXISTS dim_lifestyle",
            "DROP TABLE IF EXISTS dim_medical_conditions",
            "DROP TABLE IF EXISTS dim_healthcare_access",
        ]
        for statement in drop_statements:
            cursor.execute(statement)

        create_statements = [
            """
            CREATE TABLE dim_demographics (
                demographic_id INT PRIMARY KEY,
                sex VARCHAR(10) NOT NULL,
                age_group DECIMAL(3,1) NOT NULL,
                education_level DECIMAL(3,1) NOT NULL,
                income_bracket DECIMAL(3,1) NOT NULL,
                INDEX idx_sex (sex),
                INDEX idx_age_group (age_group),
                INDEX idx_education (education_level),
                INDEX idx_income (income_bracket)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE dim_lifestyle (
                lifestyle_id INT PRIMARY KEY,
                smoker_status DECIMAL(3,1) NOT NULL,
                physical_activity DECIMAL(3,1) NOT NULL,
                fruits_consumption DECIMAL(3,1) NOT NULL,
                vegetables_consumption DECIMAL(3,1) NOT NULL,
                heavy_alcohol_consumption DECIMAL(3,1) NOT NULL,
                INDEX idx_smoker (smoker_status),
                INDEX idx_physical_activity (physical_activity)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE dim_medical_conditions (
                medical_conditions_id INT PRIMARY KEY,
                high_blood_pressure DECIMAL(3,1) NOT NULL,
                high_cholesterol DECIMAL(3,1) NOT NULL,
                cholesterol_check DECIMAL(3,1) NOT NULL,
                stroke_history DECIMAL(3,1) NOT NULL,
                heart_disease_or_attack DECIMAL(3,1) NOT NULL,
                difficulty_walking DECIMAL(3,1) NOT NULL,
                INDEX idx_high_bp (high_blood_pressure),
                INDEX idx_high_chol (high_cholesterol),
                INDEX idx_heart_disease (heart_disease_or_attack)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE dim_healthcare_access (
                healthcare_access_id INT PRIMARY KEY,
                any_healthcare_coverage DECIMAL(3,1) NOT NULL,
                no_doctor_due_to_cost DECIMAL(3,1) NOT NULL,
                INDEX idx_healthcare_coverage (any_healthcare_coverage),
                INDEX idx_doctor_cost (no_doctor_due_to_cost)
            ) ENGINE=InnoDB
            """,
            """
            CREATE TABLE fact_health_records (
                record_id BIGINT PRIMARY KEY,
                diabetes_status DECIMAL(3,1) NOT NULL,
                bmi_value DECIMAL(5,2) NOT NULL,
                mental_health_days DECIMAL(3,1) NOT NULL,
                physical_health_days DECIMAL(3,1) NOT NULL,
                general_health_score DECIMAL(3,1) NOT NULL,
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
                INDEX idx_diabetes_bmi (diabetes_status, bmi_value)
            ) ENGINE=InnoDB
            """,
        ]
        for statement in create_statements:
            cursor.execute(statement)

        connection.commit()
        cursor.close()
        connection.close()
        logging.info("All tables created successfully")

    except mysql.connector.Error as err:
        logging.error(f"Error creating tables: {err}")
        raise


class MySQLLoader:
    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        self.config = (
            DB_CONFIG
            if host is None
            else {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "database": database,
                "charset": "utf8mb4",
                "autocommit": True,
            }
        )
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            logging.info(f"Connected to MySQL database: {self.config['database']}")
            return True
        except Error as e:
            logging.error(f"Error connecting to MySQL: {str(e)}")
            return False

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("MySQL connection closed")

    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        if df.empty:
            logging.warning(f"No data to load for table {table_name}")
            return
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        df_clean = df.fillna(0)
        data_tuples = [tuple(row) for row in df_clean.values]
        self.cursor.executemany(insert_sql, data_tuples)
        logging.info(f"Loaded {len(df)} records into {table_name}")

    def create_database(self, database_name: str = None) -> bool:
        if database_name is None:
            database_name = self.config["database"]
        try:
            temp_config = self.config.copy()
            del temp_config["database"]
            temp_connection = mysql.connector.connect(**temp_config)
            temp_cursor = temp_connection.cursor()
            temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            temp_cursor.close()
            temp_connection.close()
            logging.info(f"Database {database_name} created/verified")
            return True
        except Error as e:
            if e.errno == 1007:
                logging.info(f"Database {database_name} already exists")
                return True
            logging.error(f"Error creating database: {str(e)}")
            return False

    def create_all_tables(self) -> bool:
        try:
            create_tables()
            return True
        except Exception as e:
            logging.error(f"Error creating tables: {str(e)}")
            return False

    def verify_data_load(self):
        tables = [
            "dim_demographics",
            "dim_lifestyle",
            "dim_medical_conditions",
            "dim_healthcare_access",
            "fact_health_records",
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