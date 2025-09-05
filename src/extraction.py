import pandas as pd
import logging
import os

def extract_data(file_path: str, year: str = '2015') -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path)
        logging.info(f"BRFSS {year} data extracted successfully")
        return df
    except Exception as e:
        logging.error(f"Error extracting BRFSS data from {file_path}: {e}")
        raise

def select_diabetes_features(df: pd.DataFrame) -> pd.DataFrame:

    desired_columns = [
        "DIABETE3",  # Target variable
        "_RFHYPE5",  # High blood pressure
        "_BMI5",  # BMI
        "SMOKE100",  # Smoking
        "_TOTINDA",  # Physical activity
        "GENHLTH",  # General health
        "SEX",
        "_AGEG5YR",  # Demographics
        "TOLDHI2",
        "_CHOLCHK",  # Cholesterol
        "CVDSTRK3",
        "_MICHD",  # Heart conditions
        "_FRTLT1",
        "_VEGLT1",  # Diet
        "_RFDRHV5",  # Heavy alcohol consumption
        "HLTHPLN1",
        "MEDCOST",  # Healthcare access
        "MENTHLTH",
        "PHYSHLTH",
        "DIFFWALK",  # Health status
        "EDUCA",
        "INCOME2",  # Demographics
    ]

    available_columns = [col for col in desired_columns if col in df.columns]
    missing_columns = [col for col in desired_columns if col not in df.columns]

    if not available_columns:
        raise ValueError("No diabetes-related columns found in dataset")

    if "DIABETE3" not in available_columns:
        raise ValueError("Target variable 'DIABETE3' not found in dataset")

    if missing_columns:
        logging.warning(f"Missing columns: {missing_columns}")

    logging.info(
        f"Selected {len(available_columns)} features "
        f"(from {len(desired_columns)} desired)."
    )

    df_selected = df[available_columns].copy()
    logging.info(f"Dataset shape after feature selection: {df_selected.shape}")

    return df_selected


def load_all_brfss_data(data_dir: str = "data/raw") -> pd.DataFrame:
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    csv_files = [
        f for f in os.listdir(data_dir) if f.endswith(".csv") and f[:-4].isdigit()
    ]
    years = [f[:-4] for f in csv_files]

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    logging.info(f"Found {len(csv_files)} CSV files for years: {', '.join(sorted(years))}")

    all_dataframes = []
    total_rows = 0

    for file, year in zip(csv_files, years):
        file_path = os.path.join(data_dir, file)
        try:
            df = extract_data(file_path, year)
            df_selected = select_diabetes_features(df)
            df_selected["DATA_YEAR"] = int(year)

            all_dataframes.append(df_selected)
            total_rows += len(df_selected)

            logging.info(f"Loaded {year}: {len(df_selected)} rows")
        except Exception as e:
            logging.warning(f"Failed to load {year}: {e}")
            continue

    if not all_dataframes:
        raise RuntimeError("No datasets could be loaded successfully")

    combined_df = pd.concat(all_dataframes, ignore_index=True)

    logging.info(
        f"Combined {len(all_dataframes)} datasets: "
        f"{len(combined_df)} rows, {len(combined_df.columns)} columns"
    )
    logging.info(f"Years included: {sorted(combined_df['DATA_YEAR'].unique())}")

    return combined_df

def load_raw_data(data_dir: str = "data/raw", load_all_years: bool = False, sample_size: int = None) -> pd.DataFrame:
    if load_all_years:
        return load_all_brfss_data(data_dir)
    else:
        # Load only 2015 data
        file_path = os.path.join(data_dir, "2015.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Raw BRFSS data not found: {file_path}")
        
        # If sample_size is specified, read only that many rows
        if sample_size:
            df = pd.read_csv(file_path, nrows=sample_size)
            logging.info(f"BRFSS 2015 sample data extracted successfully: {sample_size} rows")
        else:
            df = extract_data(file_path, "2015")
        
        return select_diabetes_features(df)