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
    diabetes_mapping = {1: 2, 2: 1, 3: 0}
    if 'DIABETE3' in df.columns:
        df['DIABETE3'] = df['DIABETE3'].map(diabetes_mapping)
        logging.info(
            f"Diabetes variable distribution: {df['DIABETE3'].value_counts().to_dict()}"
        )
    return df


def transform_binary_variables(df: pd.DataFrame) -> pd.DataFrame:
    binary_columns = [
        '_RFHYPE5', 'TOLDHI2', '_CHOLCHK', 'SMOKE100', 'CVDSTRK3',
        '_MICHD', '_TOTINDA', '_FRTLT1', '_VEGLT1', '_RFDRHV5',
        'HLTHPLN1', 'MEDCOST', 'DIFFWALK'
    ]
    for col in binary_columns:
        if col in df.columns:
            df[col] = df[col].replace({2: 0})
    logging.info("Transformed binary variables to 0/1 format")
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
        'INCOME2': list(range(1, 9))
    }
    initial_rows = len(df)
    for col, valid_values in ordinal_columns.items():
        if col in df.columns:
            df = df[df[col].isin(valid_values)]
    logging.info(
        f"Cleaned ordinal variables. Rows reduced: {initial_rows} -> {len(df)}"
    )
    return df


def transform_sex_variable(df: pd.DataFrame) -> pd.DataFrame:
    if 'SEX' in df.columns:
        sex_mapping = {1: 'Masculino', 2: 'Femenino'}
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
    df = df.rename(columns=column_mapping)
    logging.info("Renamed columns to human-readable format")
    return df


def full_transformation_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting full transformation pipeline")

    if 'Diabetes_012' in df.columns:
        logging.info("Data already in final format. Skipping transformations.")
        return df

    df = clean_missing_values(df)
    df = transform_diabetes_variable(df)
    df = transform_binary_variables(df)
    df = transform_continuous_variables(df)
    df = clean_ordinal_variables(df)
    df = transform_sex_variable(df)
    df = rename_columns(df)

    logging.info(f"Transformation pipeline completed. Final shape: {df.shape}")
    return df