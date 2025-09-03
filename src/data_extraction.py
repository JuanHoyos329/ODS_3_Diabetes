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
    """
    Select diabetes-related features from BRFSS dataset.
    Handles missing columns gracefully for different years.
    """
    # Priority columns (most important)
    priority_columns = [
        'DIABETE3',           # Target variable - diabetes status
        '_RFHYPE5',           # High blood pressure
        '_BMI5',              # BMI
        'SMOKE100',           # Smoking
        '_TOTINDA',           # Physical activity
        'GENHLTH',            # General health
        'SEX', '_AGEG5YR',    # Demographics
    ]
    
    # Additional desired columns
    additional_columns = [
        'TOLDHI2', '_CHOLCHK', # Cholesterol
        'CVDSTRK3', '_MICHD', # Heart conditions
        '_FRTLT1', '_VEGLT1', # Diet
        '_RFDRHV5',           # Heavy alcohol consumption
        'HLTHPLN1', 'MEDCOST', # Healthcare access
        'MENTHLTH', 'PHYSHLTH', 'DIFFWALK', # Health status
        'EDUCA', 'INCOME2' # Demographics
    ]
    
    # All desired columns
    desired_columns = priority_columns + additional_columns
    
    try:
        # Check which columns are available
        available_columns = [col for col in desired_columns if col in df.columns]
        missing_columns = [col for col in desired_columns if col not in df.columns]
        
        if not available_columns:
            raise ValueError("No diabetes-related columns found in dataset")
        
        # Log information about column availability
        logging.info(f"Available columns: {len(available_columns)} out of {len(desired_columns)} desired")
        if missing_columns:
            logging.warning(f"Missing columns: {missing_columns}")
        
        # Check if we have the essential target variable
        if 'DIABETE3' not in available_columns:
            raise ValueError("Target variable 'DIABETE3' not found in dataset")
        
        df_selected = df[available_columns].copy()
        logging.info(f"Selected {len(available_columns)} diabetes-related features from {df.shape[1]} total columns")
        logging.info(f"Dataset shape after feature selection: {df_selected.shape}")
        
        return df_selected
        
    except KeyError as e:
        logging.error(f"Error accessing columns in dataset: {str(e)}")
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

def load_all_brfss_data(data_dir: str = 'data/raw') -> pd.DataFrame:
    """Load and combine all available BRFSS datasets from multiple years"""
    logging.info("Loading all available BRFSS data files...")
    
    # Get all CSV files in the directory
    csv_files = []
    years = []
    
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    # Find all CSV files that look like year files
    for file in os.listdir(data_dir):
        if file.endswith('.csv') and file.replace('.csv', '').isdigit():
            year = file.replace('.csv', '')
            csv_files.append(os.path.join(data_dir, file))
            years.append(year)
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    
    logging.info(f"Found {len(csv_files)} CSV files for years: {', '.join(sorted(years))}")
    
    # Load and combine all datasets
    all_dataframes = []
    total_rows = 0
    
    for file_path, year in zip(csv_files, years):
        try:
            logging.info(f"Loading {year} dataset from {file_path}...")
            df = pd.read_csv(file_path)
            
            # Add year column to track data source
            df['DATA_YEAR'] = int(year)
            
            # Select diabetes features
            df_selected = select_diabetes_features(df)
            
            # Re-add the year column after feature selection
            df_selected['DATA_YEAR'] = int(year)
            
            all_dataframes.append(df_selected)
            total_rows += len(df_selected)
            
            logging.info(f"✅ Successfully loaded {year}: {len(df_selected):,} rows")
            
        except Exception as e:
            logging.warning(f"⚠️  Failed to load {year} dataset: {str(e)}")
            continue
    
    if not all_dataframes:
        raise Exception("No datasets could be loaded successfully")
    
    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    logging.info(f"✅ Successfully combined {len(all_dataframes)} datasets")
    logging.info(f"Combined dataset: {len(combined_df):,} rows, {len(combined_df.columns)} columns")
    logging.info(f"Years included: {sorted(combined_df['DATA_YEAR'].unique().tolist())}")
    
    return combined_df

def load_raw_data(data_dir: str = 'data/raw', load_all_years: bool = True) -> pd.DataFrame:
    """
    Load raw BRFSS data for processing.
    
    Args:
        data_dir: Directory containing the raw CSV files
        load_all_years: If True, loads all available years. If False, loads only 2015.
    
    Returns:
        pd.DataFrame: Combined dataset from all years or single year
    """
    logging.info("Loading raw BRFSS data for processing...")
    
    if load_all_years:
        return load_all_brfss_data(data_dir)
    else:
        return load_raw_brfss_data(data_dir, '2015')