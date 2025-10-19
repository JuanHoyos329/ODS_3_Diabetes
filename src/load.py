import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG_MORTALITY


def create_mortality_tables(connection):
  
    cursor = connection.cursor()
    
    try:
        logging.info("Creating tables for child mortality dimensional model")
        
        # Drop existing tables (in correct order due to foreign keys)
        drop_statements = [
            "DROP TABLE IF EXISTS fact_child_health",
            "DROP TABLE IF EXISTS dim_time",
            "DROP TABLE IF EXISTS dim_mortality_indicators",
            "DROP TABLE IF EXISTS dim_socioeconomic_indicators"
        ]
        
        for statement in drop_statements:
            cursor.execute(statement)
        
        # Create dimension tables
        create_statements = [
            # Time dimension
            """
            CREATE TABLE dim_time (
                time_id INT PRIMARY KEY,
                year INT NOT NULL,
                decade INT NOT NULL,
                period VARCHAR(20) NOT NULL,
                INDEX idx_year (year),
                INDEX idx_decade (decade)
            ) ENGINE=InnoDB
            """,
            
            # Mortality indicators dimension
            """
            CREATE TABLE dim_mortality_indicators (
                mortality_indicator_id INT PRIMARY KEY,
                infant_mortality_rate DECIMAL(10,4),
                under_five_mortality_rate DECIMAL(10,4),
                neonatal_mortality_rate DECIMAL(10,4),
                child_5_14_mortality_rate DECIMAL(10,4),
                adolescent_mortality_rate DECIMAL(10,4),
                infant_deaths_count DECIMAL(15,2),
                under_five_deaths_count DECIMAL(15,2),
                adolescent_deaths_count DECIMAL(15,2),
                INDEX idx_infant_mortality (infant_mortality_rate),
                INDEX idx_under_five_mortality (under_five_mortality_rate)
            ) ENGINE=InnoDB
            """,
            
            # Socioeconomic indicators dimension
            """
            CREATE TABLE dim_socioeconomic_indicators (
                socioeconomic_indicator_id INT PRIMARY KEY,
                population_total DECIMAL(15,2),
                gdp_per_capita DECIMAL(15,2),
                poverty_headcount_ratio DECIMAL(10,4),
                school_enrollment_primary DECIMAL(10,4),
                mortality_rate DECIMAL(10,4),
                urban_population_percent DECIMAL(10,4),
                INDEX idx_poverty (poverty_headcount_ratio),
                INDEX idx_gdp (gdp_per_capita)
            ) ENGINE=InnoDB
            """,
            
            # Fact table
            """
            CREATE TABLE fact_child_health (
                record_id BIGINT PRIMARY KEY,
                time_id INT NOT NULL,
                mortality_indicator_id INT NOT NULL,
                socioeconomic_indicator_id INT NOT NULL,
                infant_mortality_rate DECIMAL(10,4),
                under_five_mortality_rate DECIMAL(10,4),
                neonatal_mortality_rate DECIMAL(10,4),
                FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
                FOREIGN KEY (mortality_indicator_id) REFERENCES dim_mortality_indicators(mortality_indicator_id),
                FOREIGN KEY (socioeconomic_indicator_id) REFERENCES dim_socioeconomic_indicators(socioeconomic_indicator_id),
                INDEX idx_time (time_id),
                INDEX idx_mortality (mortality_indicator_id),
                INDEX idx_infant_rate (infant_mortality_rate),
                INDEX idx_under_five_rate (under_five_mortality_rate)
            ) ENGINE=InnoDB
            """
        ]
        
        for i, statement in enumerate(create_statements, 1):
            cursor.execute(statement)
            logging.info(f"  [{i}/{len(create_statements)}] Table created")
        
        connection.commit()
        logging.info("All tables created successfully")
        
    except Error as e:
        logging.error(f"Error creating tables: {e}")
        raise
    finally:
        cursor.close()


class MySQLLoaderMortality:
    
    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        """
        Initialize MySQL loader.
        
        Args:
            host: Database host
            port: Database port
            user: Database user
            password: Database password
            database: Database name
        """
        self.config = (
            DB_CONFIG_MORTALITY.copy()
            if host is None
            else {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "database": database,
            }
        )
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        Connect to MySQL database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            logging.info(f"Connected to MySQL database: {self.config['database']}")
            return True
        except Error as e:
            logging.error(f"Error connecting to MySQL: {str(e)}")
            return False
    
    def disconnect(self):
        """
        Disconnect from MySQL database.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("MySQL connection closed")
    
    def create_database(self, database_name: str = None) -> bool:
        """
        Create database if it doesn't exist.
        
        Args:
            database_name: Name of database to create
            
        Returns:
            True if successful, False otherwise
        """
        if database_name is None:
            database_name = self.config["database"]
        
        try:
            # Connect without database
            temp_config = self.config.copy()
            temp_config.pop('database', None)
            
            temp_connection = mysql.connector.connect(**temp_config)
            temp_cursor = temp_connection.cursor()
            
            temp_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            
            temp_cursor.close()
            temp_connection.close()
            
            logging.info(f"Database {database_name} created/verified")
            return True
            
        except Error as e:
            logging.error(f"Error creating database: {str(e)}")
            return False
    
    def create_all_tables(self) -> bool:
        """
        Create all tables for dimensional model.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            create_mortality_tables(self.connection)
            return True
        except Exception as e:
            logging.error(f"Error creating tables: {str(e)}")
            return False
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Load DataFrame to MySQL table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
        """
        if df.empty:
            logging.warning(f"No data to load for table {table_name}")
            return
        
        # Replace NaN with None for MySQL NULL
        df_clean = df.where(pd.notnull(df), None)
        
        # Generate INSERT statement
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Convert to list of tuples
        data_tuples = [tuple(row) for row in df_clean.values]
        
        try:
            # Execute batch insert
            self.cursor.executemany(insert_sql, data_tuples)
            self.connection.commit()
            logging.info(f"Loaded {len(df)} records to {table_name}")
        except Error as e:
            logging.error(f"Error loading data to {table_name}: {e}")
            self.connection.rollback()
            raise
    
    def load_dimensional_tables(self, tables: dict):
        """
        Load all dimensional tables to database.
        
        Args:
            tables: Dictionary of table_name: DataFrame
        """
        logging.info("=" * 70)
        logging.info("LOADING DATA TO MYSQL DATABASE")
        logging.info("=" * 70)
        
        # Define load order (dimensions first, then fact table)
        load_order = [
            'dim_time',
            'dim_mortality_indicators',
            'dim_socioeconomic_indicators',
            'fact_child_health'
        ]
        
        for table_name in load_order:
            if table_name in tables:
                logging.info(f"\nLoading {table_name}...")
                self.load_dataframe(tables[table_name], table_name)
            else:
                logging.warning(f"Table {table_name} not found in provided tables")
        
        logging.info("\n" + "=" * 70)
        logging.info("ALL DATA LOADED SUCCESSFULLY")
        logging.info("=" * 70)
    
    def verify_data_load(self) -> dict:
        """
        Verify data was loaded correctly by counting records in each table.
        
        Returns:
            Dictionary with table names and record counts
        """
        tables = [
            "dim_time",
            "dim_mortality_indicators",
            "dim_socioeconomic_indicators",
            "fact_child_health"
        ]
        
        counts = {}
        
        logging.info("\nVerifying data load:")
        
        try:
            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                counts[table] = count
                logging.info(f"  {table}: {count:,} records")
            
            return counts
            
        except Error as e:
            logging.error(f"Error verifying data load: {str(e)}")
            return {}
    
    def execute_query(self, query: str) -> list:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of result tuples
        """
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except Error as e:
            logging.error(f"Error executing query: {e}")
            return []
