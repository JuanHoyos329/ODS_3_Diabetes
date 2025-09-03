# Load BRFSS diabetes data from CSV files

import pandas as pd
import logging
import os

def extract_brfss_data(file_path: str, year: str = '2015') -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully extracted BRFSS {year} data: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        logging.error(f"Error extracting BRFSS data from {file_path}: {str(e)}")
        raise

def select_diabetes_features(df: pd.DataFrame) -> pd.DataFrame:
    selected_columns = [
        'DIABETE3',           # Target variable - diabetes status
        '_RFHYPE5',           # High blood pressure
        'TOLDHI2', '_CHOLCHK', # Cholesterol
        '_BMI5',              # BMI
        'SMOKE100',           # Smoking
        'CVDSTRK3', '_MICHD', # Heart conditions
        '_TOTINDA',           # Physical activity
        '_FRTLT1', '_VEGLT1', # Diet
        '_RFDRHV5',           # Heavy alcohol consumption
        'HLTHPLN1', 'MEDCOST', # Healthcare access
        'GENHLTH', 'MENTHLTH', 'PHYSHLTH', 'DIFFWALK', # Health status
        'SEX', '_AGEG5YR', 'EDUCA', 'INCOME2' # Demographics
    ]
    
    try:
        df_selected = df[selected_columns].copy()
        logging.info(f"Selected {len(selected_columns)} diabetes-related features from {df.shape[1]} total columns")
        logging.info(f"Dataset shape after feature selection: {df_selected.shape}")
        return df_selected
    except KeyError as e:
        logging.error(f"Missing columns in dataset: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error selecting features: {str(e)}")
        raise

def load_raw_brfss_data(data_dir: str = 'data/raw', year: str = '2015') -> pd.DataFrame:
    file_path = os.path.join(data_dir, f'{year}.csv')
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Could not find raw BRFSS data at {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully loaded raw BRFSS {year} dataset: {df.shape[0]} rows, {df.shape[1]} columns")
        df_selected = select_diabetes_features(df)
        return df_selected
    except Exception as e:
        logging.error(f"Error loading raw BRFSS dataset from {file_path}: {str(e)}")
        raise

def load_raw_data(data_dir: str = 'data/raw') -> pd.DataFrame:
    logging.info("Loading raw BRFSS data for processing...")
    return load_raw_brfss_data(data_dir, '2015')