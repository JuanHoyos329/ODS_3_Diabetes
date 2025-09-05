import pandas as pd
import logging

def clean_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    initial_rows = len(df)
    df = df.dropna()
    logging.info(
        f"Dropped missing values: {initial_rows} -> {len(df)} rows "
        f"({initial_rows - len(df)} removed)"
    )
    return df

def transform_diabetes_variable(df: pd.DataFrame) -> pd.DataFrame:
    diabetes_mapping = {1: "Diabetic", 3: "Healthy", 2: "Prediabetic"}

    if 'DIABETE3' in df.columns:
        # Show unique values before transformation
        unique_vals = sorted(df['DIABETE3'].dropna().unique())
        logging.info(f"DIABETE3 - unique values before transformation: {unique_vals}")

        # Filter only valid values (1, 2, 3) before mapping
        initial_rows = len(df)
        df = df[df['DIABETE3'].isin([1, 2, 3])].copy()
        filtered_rows = len(df)

        # Apply mapping to descriptive strings
        df['DIABETE3'] = df['DIABETE3'].map(diabetes_mapping)

        # Show result
        final_vals = sorted(df['DIABETE3'].dropna().unique())
        logging.info(f"DIABETE3 - unique values after transformation: {final_vals}")
        logging.info(f"Diabetes variable transformation: {initial_rows} -> {filtered_rows} rows "
                    f"({initial_rows - filtered_rows} rows with invalid values removed)")
        logging.info(f"Diabetes variable distribution: {df['DIABETE3'].value_counts().to_dict()}")
    
    return df

def transform_binary_variables(df: pd.DataFrame) -> pd.DataFrame:
    binary_columns = [
        '_RFHYPE5', 'TOLDHI2', '_CHOLCHK', 'SMOKE100', 'CVDSTRK3',
        '_MICHD', '_TOTINDA', '_FRTLT1', '_VEGLT1', '_RFDRHV5',
        'HLTHPLN1', 'MEDCOST', 'DIFFWALK'
    ]
    
    initial_rows = len(df)
    
    for col in binary_columns:
        if col in df.columns:
            # Show unique values before filtering
            unique_vals = sorted(df[col].dropna().unique())
            logging.info(f"Column {col} - unique values before cleaning: {unique_vals}")

            # Check if values are binary (0,1) or (1,2)
            valid_values = set(df[col].dropna().unique())
            
            if valid_values.issubset({0, 1}):
                # Already in 0,1 format - convert to string
                df = df[df[col].isin([0, 1])].copy()
                df[col] = df[col].map({0: "No", 1: "Yes"})
                logging.info(f"Column {col} - converted from 0,1 to No,Yes")
                
            elif valid_values.issubset({1, 2}):
                df = df[df[col].isin([1, 2])].copy()
                df[col] = df[col].replace({2: 0}).map({0: "No", 1: "Yes"})
                logging.info(f"Column {col} - converted from 1,2 to No,Yes")

            else:
                if {0, 1}.issubset(valid_values):
                    df = df[df[col].isin([0, 1])].copy() 
                    df[col] = df[col].map({0: "No", 1: "Yes"})
                    logging.info(f"Column {col} - filtered to 0,1 and converted to No,Yes")
                elif {1, 2}.issubset(valid_values):
                    df = df[df[col].isin([1, 2])].copy() 
                    df[col] = df[col].replace({2: 0}).map({0: "No", 1: "Yes"})
                    logging.info(f"Column {col} - filtered to 1,2 and converted to No,Yes")
                else:
                    logging.warning(f"Column {col} - No valid binary pattern found: {valid_values}")
                    continue
            
            final_vals = sorted(df[col].dropna().unique())
            logging.info(f"Column {col} - unique values after cleaning: {final_vals}")
            
    final_rows = len(df)
    logging.info(f"Binary variables transformation: {initial_rows} -> {final_rows} rows "
                f"({initial_rows - final_rows} rows with invalid values removed)")
    
    return df


def transform_continuous_variables(df: pd.DataFrame) -> pd.DataFrame:
    if '_BMI5' in df.columns:
        df['_BMI5'] = df['_BMI5'] / 100
    logging.info("Transformed continuous variables (BMI adjusted)")
    return df


def clean_ordinal_variables(df: pd.DataFrame) -> pd.DataFrame:
    ordinal_columns = {
        'GENHLTH': [1, 2, 3, 4, 5],
        'SEX': [1, 2],
        '_AGEG5YR': list(range(1, 14)),
        'EDUCA': list(range(1, 7)),
        'INCOME2': list(range(1, 9)),
        'MENTHLTH': list(range(0, 31)),
        'PHYSHLTH': list(range(0, 31))
    }
    
    initial_rows = len(df)
    
    for col, valid_values in ordinal_columns.items():
        if col in df.columns:
            # Show unique values before cleaning
            unique_vals = sorted(df[col].dropna().unique())
            logging.info(f"Column {col} - unique values before cleaning: {unique_vals[:10]}...")

            # Filter only valid values
            df = df[df[col].isin(valid_values)].copy()
            
            # Show final values
            final_vals = sorted(df[col].dropna().unique())
            logging.info(f"Column {col} - unique values after cleaning: {final_vals[:10]}...")
    
    final_rows = len(df)
    logging.info(f"Ordinal variables cleaning: {initial_rows} -> {final_rows} rows "
               f"({initial_rows - final_rows} rows with invalid values removed)")
    
    return df

def transform_sex_variable(df: pd.DataFrame) -> pd.DataFrame:
    if 'SEX' in df.columns:
        sex_mapping = {1: 'Male', 2: 'Female'}
        df['SEX'] = df['SEX'].map(sex_mapping)
        logging.info(
            f"Sex variable distribution: {df['SEX'].value_counts().to_dict()}"
        )
        if df['SEX'].isna().any():
            logging.warning(
                f"Unmapped values in SEX: {df['SEX'].isna().sum()} rows"
            )
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    column_mapping = {
        'DIABETE3': 'diabetes_status',
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

    df = df.rename(columns=column_mapping)
    logging.info("Renamed columns to human-readable format")
    return df


def validate_final_data(df: pd.DataFrame) -> pd.DataFrame:

    logging.info("Starting final data validation...")
    
    expected_ranges = {
        'diabetes_status': ["Diabetic", "Healthy", "Prediabetic"],
        'HighBP': ["No", "Yes"],
        'HighChol': ["No", "Yes"],
        'CholCheck': ["No", "Yes"],
        'Smoker': ["No", "Yes"],
        'Stroke': ["No", "Yes"],
        'HeartDiseaseorAttack': ["No", "Yes"],
        'PhysActivity': ["No", "Yes"],
        'Fruits': ["No", "Yes"],
        'Veggies': ["No", "Yes"],
        'HvyAlcoholConsump': ["No", "Yes"],
        'AnyHealthcare': ["No", "Yes"],
        'NoDocbcCost': ["No", "Yes"],
        'DiffWalk': ["No", "Yes"],
        'GenHlth': [1, 2, 3, 4, 5],
        'MentHlth': list(range(0, 31)),
        'PhysHlth': list(range(0, 31)),
        'Sex': ['Male', 'Female'],
        'Age': list(range(1, 14)),
        'Education': list(range(1, 7)),
        'Income': list(range(1, 9))
    }
    
    initial_rows = len(df)
    validation_issues = []
    
    for col, valid_values in expected_ranges.items():
        if col in df.columns:
            
            if col in ['Sex', 'diabetes_status', 'HighBP', 'HighChol', 'CholCheck', 'Smoker', 'Stroke', 
                      'HeartDiseaseorAttack', 'PhysActivity', 'Fruits', 'Veggies', 'HvyAlcoholConsump', 
                      'AnyHealthcare', 'NoDocbcCost', 'DiffWalk']:
                invalid_mask = ~df[col].isin(valid_values)
            else:  # Numeric columns
                invalid_mask = ~df[col].isin(valid_values)
                
            invalid_count = invalid_mask.sum()
            
            if invalid_count > 0:
                unique_invalid = df.loc[invalid_mask, col].unique()
                validation_issues.append(f"{col}: {invalid_count} invalid values {list(unique_invalid)}")
                logging.warning(f"Found {invalid_count} invalid values in {col}: {list(unique_invalid)}")
                
                # Remove rows with invalid values
                df = df[~invalid_mask].copy()
    
    final_rows = len(df)
    
    if validation_issues:
        logging.warning(f"Data validation found issues: {validation_issues}")
        logging.info(f"Removed {initial_rows - final_rows} rows with invalid values")
    else:
        logging.info("All data values are within expected ranges")
    
    logging.info("Final data validation summary:")
    for col in df.columns:
        if col in expected_ranges:
            if col in ['Sex', 'diabetes_status', 'HighBP', 'HighChol', 'CholCheck', 'Smoker', 'Stroke', 
                      'HeartDiseaseorAttack', 'PhysActivity', 'Fruits', 'Veggies', 'HvyAlcoholConsump', 
                      'AnyHealthcare', 'NoDocbcCost', 'DiffWalk']:
                unique_vals = df[col].unique()
            else:
                unique_vals = sorted(df[col].unique())
            logging.info(f"  {col}: {list(unique_vals)}")
    
    logging.info(f"Final validated dataset: {df.shape}")
    return df

def full_transformation_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting full transformation pipeline")

    if 'diabetes_status' in df.columns:
        logging.info("Data already in final format. Applying validation only.")
        return validate_final_data(df)

    df = clean_missing_values(df)
    df = transform_diabetes_variable(df)
    df = transform_binary_variables(df)
    df = transform_continuous_variables(df)
    df = clean_ordinal_variables(df)
    df = transform_sex_variable(df)
    df = rename_columns(df)
    df = validate_final_data(df)

    logging.info(f"Transformation pipeline completed. Final shape: {df.shape}")
    return df