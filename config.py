import os

# Directory paths
DATA_DIR = 'data'
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
FINAL_DATA_DIR = os.path.join(DATA_DIR, 'final')
LOGS_DIR = 'logs'

# File names
RAW_DATA_FILE = 'diabetes_012_health_indicators_BRFSS2015.csv'
FINAL_DATA_FILE = 'diabetes_012_health_indicators_BRFSS2015.csv'
SUMMARY_FILE = 'dataset_summary.txt'

# Data processing configuration
RANDOM_SEED = 1
LOG_LEVEL = 'INFO'

# Feature categories for analysis
FEATURE_CATEGORIES = {
    'demographics': ['Sex', 'Age', 'Education', 'Income'],
    'health_conditions': ['HighBP', 'HighChol', 'Stroke', 'HeartDiseaseorAttack'],
    'lifestyle': ['Smoker', 'PhysActivity', 'Fruits', 'Veggies', 'HvyAlcoholConsump'],
    'healthcare': ['AnyHealthcare', 'NoDocbcCost', 'CholCheck'],
    'health_status': ['GenHlth', 'MentHlth', 'PhysHlth', 'DiffWalk', 'BMI']
}

# Target variable information
TARGET_VARIABLE = 'diabetes_status'
TARGET_CLASSES = {
    0: 'Healthy',
    1: 'Pre-diabetic', 
    2: 'Diabetic'
}

# Expected dataset dimensions after processing
EXPECTED_FINAL_ROWS = 253680
EXPECTED_FINAL_COLS = 22

# MySQL Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "diabetesDB",
    "port": 3306,
}

# Database table names
DB_TABLES = {
    'dim_demographics': 'dim_demographics',
    'dim_lifestyle': 'dim_lifestyle', 
    'dim_medical_conditions': 'dim_medical_conditions',
    'dim_healthcare_access': 'dim_healthcare_access',
    'fact_health_records': 'fact_health_records'
}

# Database views
DB_VIEWS = [
    'vw_diabetes_summary',
    'vw_demographics_analysis',
    'vw_lifestyle_impact'
]