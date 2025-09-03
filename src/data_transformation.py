import pandas as pd
import numpy as np
import logging
import random

def clean_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    initial_rows = len(df)
    df_cleaned = df.dropna()
    final_rows = len(df_cleaned)
    
    logging.info(f"Dropped missing values: {initial_rows} -> {final_rows} rows ({initial_rows - final_rows} removed)")
    return df_cleaned

def transform_diabetes_variable(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    
    # Keep original BRFSS coding: 1=Yes diagnosed, 2=Yes pre-diabetes, 3=No
    # Transform to: 0=No diabetes, 1=Pre-diabetes, 2=Diabetes
    diabetes_mapping = {1: 2, 2: 1, 3: 0}
    df_copy['DIABETE3'] = df_copy['DIABETE3'].map(diabetes_mapping)
    
    logging.info(f"Diabetes variable distribution: {df_copy['DIABETE3'].value_counts().to_dict()}")
    return df_copy

def transform_binary_variables(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    
    # Binary variables that need 2->0 transformation
    binary_columns = ['_RFHYPE5', 'TOLDHI2', '_CHOLCHK', 'SMOKE100', 'CVDSTRK3', 
                     '_MICHD', '_TOTINDA', '_FRTLT1', '_VEGLT1', '_RFDRHV5', 
                     'HLTHPLN1', 'MEDCOST', 'DIFFWALK']
    
    for col in binary_columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].replace({2: 0})
    
    logging.info("Transformed all binary variables to 0/1 format")
    return df_copy

def transform_continuous_variables(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    
    # BMI transformation (divide by 100)
    if '_BMI5' in df_copy.columns:
        df_copy['_BMI5'] = df_copy['_BMI5'] / 100
    
    # Mental and Physical Health (30 = None, keep as 30)
    # No transformation needed as per notebook
    
    logging.info("Transformed continuous variables (BMI, mental health, physical health)")
    return df_copy

def clean_ordinal_variables(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    
    # Remove invalid responses (7, 9) from ordinal variables
    ordinal_columns = {
        'GENHLTH': [1, 2, 3, 4, 5],
        'SEX': [1, 2],
        '_AGEG5YR': list(range(1, 14)),
        'EDUCA': list(range(1, 7)),
        'INCOME2': list(range(1, 9))
    }
    
    initial_rows = len(df_copy)
    
    for col, valid_values in ordinal_columns.items():
        if col in df_copy.columns:
            df_copy = df_copy[df_copy[col].isin(valid_values)]
    
    final_rows = len(df_copy)
    logging.info(f"Cleaned ordinal variables by removing invalid responses")
    logging.info(f"Dataset shape after cleaning: ({final_rows}, {df_copy.shape[1]}) - removed {initial_rows - final_rows} rows")
    
    return df_copy

def transform_sex_variable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform SEX variable from BRFSS coding to descriptive text.
    BRFSS coding: 1 = Male, 2 = Female
    Database will store: 'Masculino', 'Femenino'
    """
    df_copy = df.copy()
    
    if 'SEX' in df_copy.columns:
        # Create mapping from BRFSS to descriptive text
        sex_mapping = {
            1: 'Masculino',  # Male -> Masculino
            2: 'Femenino'    # Female -> Femenino
        }
        
        # Apply the transformation
        df_copy['SEX'] = df_copy['SEX'].map(sex_mapping)
        
        # Log the distribution
        sex_counts = df_copy['SEX'].value_counts()
        logging.info(f"Sex variable distribution after transformation: {sex_counts.to_dict()}")
        
        # Check for any unmapped values
        if df_copy['SEX'].isna().any():
            unmapped_count = df_copy['SEX'].isna().sum()
            logging.warning(f"Found {unmapped_count} unmapped values in SEX variable")
    else:
        logging.warning("SEX column not found in dataset")
    
    return df_copy

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df_renamed = df.copy()
    
    column_mapping = {
        'DIABETE3': 'Diabetes_012',
        '_RFHYPE5': 'HighBP',
        'TOLDHI2': 'HighChol',
        '_CHOLCHK': 'CholCheck',
        '_BMI5': 'BMI',
        'SMOKE100': 'Smoker',
        'CVDSTRK3': 'Stroke',
        '_MICHD': 'HeartDiseaseorAttack',
        '_TOTINDA': 'PhysActivity',
        '_FRTLT1': 'Fruits',
        '_VEGLT1': 'Veggies',
        '_RFDRHV5': 'HvyAlcoholConsump',
        'HLTHPLN1': 'AnyHealthcare',
        'MEDCOST': 'NoDocbcCost',
        'GENHLTH': 'GenHlth',
        'MENTHLTH': 'MentHlth',
        'PHYSHLTH': 'PhysHlth',
        'DIFFWALK': 'DiffWalk',
        'SEX': 'Sex',
        '_AGEG5YR': 'Age',
        'EDUCA': 'Education',
        'INCOME2': 'Income'
    }
    
    df_renamed = df_renamed.rename(columns=column_mapping)
    logging.info("Renamed columns to more readable format")
    return df_renamed

def full_transformation_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting full transformation pipeline")
    
    # Check if data is already in final format
    if 'Diabetes_012' in df.columns:
        logging.info("Data already in final format - skipping transformation")
        logging.info(f"Final dataset shape: {df.shape}")
        return df
    
    # Apply transformations step by step
    df = clean_missing_values(df)
    df = transform_diabetes_variable(df)
    df = transform_binary_variables(df)
    df = transform_continuous_variables(df)
    df = clean_ordinal_variables(df)
    df = transform_sex_variable(df) 
    df = rename_columns(df)
    
    logging.info(f"Main dataset final shape: {df.shape}")
    logging.info("Transformation pipeline completed successfully")
    
    return df