import pandas as pd
import logging
import os

def create_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    
    logging.info("Creating time dimension")
    
    # Get unique years
    years = sorted(df['year'].unique())
    
    # Create time dimension
    dim_time = pd.DataFrame({
        'time_id': range(1, len(years) + 1),
        'year': years,
        'decade': [(year // 10) * 10 for year in years],
        'period': ['Historical' if year < 2010 else 'Recent' for year in years]
    })
    
    logging.info(f"Time dimension created: {len(dim_time)} records")
    logging.info(f"Year range: {dim_time['year'].min()} - {dim_time['year'].max()}")
    
    return dim_time


def create_dim_mortality_indicators(df: pd.DataFrame) -> pd.DataFrame:
    
    logging.info("Creating mortality indicators dimension")
    
    # Define expected mortality indicator columns (matching MySQL table structure)
    expected_cols = [
        'infant_mortality_rate',
        'under_five_mortality_rate',
        'neonatal_mortality_rate',
        'child_5_14_mortality_rate',
        'adolescent_mortality_rate',
        'infant_deaths_count',
        'under_five_deaths_count',
        'adolescent_deaths_count'
    ]
    
    # Find available columns in the DataFrame
    available_cols = [col for col in expected_cols if col in df.columns]
    
    if not available_cols:
        logging.warning("No mortality indicator columns found")
        # Return a minimal dimension with all columns as None
        dim_data = {'mortality_indicator_id': [1]}
        for col in expected_cols:
            dim_data[col] = [None]
        return pd.DataFrame(dim_data)
    
    # Create DataFrame with only available columns
    df_mortality = df[available_cols].copy()
    
    # Add missing columns with None values
    for col in expected_cols:
        if col not in df_mortality.columns:
            df_mortality[col] = None
    
    # Reorder columns to match MySQL table
    df_mortality = df_mortality[expected_cols]
    
    # Remove duplicates
    df_mortality = df_mortality.drop_duplicates().reset_index(drop=True)
    
    # Add ID column at the beginning
    df_mortality.insert(0, 'mortality_indicator_id', range(1, len(df_mortality) + 1))
    
    logging.info(f"Mortality indicators dimension created: {len(df_mortality)} records")
    logging.info(f"Available indicators: {available_cols}")
    logging.info(f"Missing indicators filled with None: {[col for col in expected_cols if col not in available_cols]}")
    
    return df_mortality


def create_dim_socioeconomic_indicators(df: pd.DataFrame) -> pd.DataFrame:
    
    logging.info("Creating socioeconomic indicators dimension")
    
    # Define expected socioeconomic indicator columns (matching MySQL table structure)
    expected_cols = [
        'population_total',
        'gdp_per_capita',
        'poverty_headcount_ratio',
        'school_enrollment_primary',
        'mortality_rate',
        'urban_population_percent'
    ]
    
    # Find available columns in the DataFrame
    available_cols = [col for col in expected_cols if col in df.columns]
    
    if not available_cols:
        logging.warning("No socioeconomic indicator columns found")
        # Return a minimal dimension with all columns as None
        dim_data = {'socioeconomic_indicator_id': [1]}
        for col in expected_cols:
            dim_data[col] = [None]
        return pd.DataFrame(dim_data)
    
    # Create DataFrame with only available columns
    df_socioeconomic = df[available_cols].copy()
    
    # Add missing columns with None values
    for col in expected_cols:
        if col not in df_socioeconomic.columns:
            df_socioeconomic[col] = None
    
    # Reorder columns to match MySQL table
    df_socioeconomic = df_socioeconomic[expected_cols]
    
    # Remove duplicates
    df_socioeconomic = df_socioeconomic.drop_duplicates().reset_index(drop=True)
    
    # Add ID column at the beginning
    df_socioeconomic.insert(0, 'socioeconomic_indicator_id', range(1, len(df_socioeconomic) + 1))
    
    logging.info(f"Socioeconomic indicators dimension created: {len(df_socioeconomic)} records")
    logging.info(f"Available indicators: {available_cols}")
    logging.info(f"Missing indicators filled with None: {[col for col in expected_cols if col not in available_cols]}")
    
    return df_socioeconomic


def create_fact_child_health(
    df: pd.DataFrame,
    dim_time: pd.DataFrame,
    dim_mortality: pd.DataFrame,
    dim_socioeconomic: pd.DataFrame
) -> pd.DataFrame:
    
    logging.info("Creating fact table for child health")
    
    # Merge with time dimension to get time_id
    df_with_time = pd.merge(
        df,
        dim_time[['time_id', 'year']],
        on='year',
        how='left'
    )
    
    # Define expected mortality columns (excluding ID)
    expected_mortality_cols = [
        'infant_mortality_rate', 'under_five_mortality_rate', 'neonatal_mortality_rate',
        'child_5_14_mortality_rate', 'adolescent_mortality_rate', 'infant_deaths_count',
        'under_five_deaths_count', 'adolescent_deaths_count'
    ]
    
    # Define expected socioeconomic columns (excluding ID)
    expected_socioeconomic_cols = [
        'population_total', 'gdp_per_capita', 'poverty_headcount_ratio',
        'school_enrollment_primary', 'mortality_rate', 'urban_population_percent'
    ]
    
    # Find common mortality columns between df and dimension
    mortality_merge_cols = [
        col for col in expected_mortality_cols 
        if col in df.columns and col in dim_mortality.columns
    ]
    
    # Find common socioeconomic columns between df and dimension
    socioeconomic_merge_cols = [
        col for col in expected_socioeconomic_cols 
        if col in df.columns and col in dim_socioeconomic.columns
    ]
    
    # Merge with mortality dimension
    if len(mortality_merge_cols) > 0 and len(dim_mortality) > 0:
        df_with_dims = pd.merge(
            df_with_time,
            dim_mortality[['mortality_indicator_id'] + mortality_merge_cols],
            on=mortality_merge_cols,
            how='left'
        )
    else:
        df_with_dims = df_with_time.copy()
        df_with_dims['mortality_indicator_id'] = 1
    
    # Merge with socioeconomic dimension
    if len(socioeconomic_merge_cols) > 0 and len(dim_socioeconomic) > 0:
        df_with_dims = pd.merge(
            df_with_dims,
            dim_socioeconomic[['socioeconomic_indicator_id'] + socioeconomic_merge_cols],
            on=socioeconomic_merge_cols,
            how='left'
        )
    else:
        df_with_dims['socioeconomic_indicator_id'] = 1
    
    # Create fact table with only IDs and key metrics
    fact_columns = {
        'record_id': range(1, len(df_with_dims) + 1),
        'time_id': df_with_dims['time_id'],
        'mortality_indicator_id': df_with_dims['mortality_indicator_id'],
        'socioeconomic_indicator_id': df_with_dims['socioeconomic_indicator_id']
    }
    
    # Add key metrics to fact table
    key_metrics = [
        'infant_mortality_rate',
        'under_five_mortality_rate',
        'neonatal_mortality_rate'
    ]
    
    for metric in key_metrics:
        if metric in df_with_dims.columns:
            fact_columns[metric] = df_with_dims[metric]
    
    fact_table = pd.DataFrame(fact_columns)
    
    logging.info(f"Fact table created: {len(fact_table)} records")
    logging.info(f"Fact table columns: {list(fact_table.columns)}")
    
    return fact_table


def dimensional_model_mortality(df_clean: pd.DataFrame) -> dict:
    
    logging.info("=" * 70)
    logging.info("CREATING DIMENSIONAL MODEL FOR CHILD MORTALITY")
    logging.info("=" * 70)
    
    # Create time dimension
    logging.info("\n[1/4] Creating time dimension...")
    dim_time = create_dim_time(df_clean)
    
    # Create mortality indicators dimension
    logging.info("\n[2/4] Creating mortality indicators dimension...")
    dim_mortality = create_dim_mortality_indicators(df_clean)
    
    # Create socioeconomic indicators dimension
    logging.info("\n[3/4] Creating socioeconomic indicators dimension...")
    dim_socioeconomic = create_dim_socioeconomic_indicators(df_clean)
    
    # Create fact table
    logging.info("\n[4/4] Creating fact table...")
    fact_child_health = create_fact_child_health(
        df_clean,
        dim_time,
        dim_mortality,
        dim_socioeconomic
    )
    
    # Package all tables
    tables = {
        'dim_time': dim_time,
        'dim_mortality_indicators': dim_mortality,
        'dim_socioeconomic_indicators': dim_socioeconomic,
        'fact_child_health': fact_child_health
    }
    
    # Summary
    logging.info("\n" + "=" * 70)
    logging.info("DIMENSIONAL MODEL CREATED")
    logging.info("=" * 70)
    for table_name, table_df in tables.items():
        logging.info(f"{table_name}: {len(table_df)} records, {len(table_df.columns)} columns")
    
    return tables


def save_dimensional_tables(tables: dict, output_dir: str = "data/processed"):
    
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    logging.info(f"Saving dimensional tables to {output_dir}")
    
    for table_name, df in tables.items():
        file_path = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(file_path, index=False)
        logging.info(f"  Saved {table_name}: {len(df)} records -> {file_path}")
    
    logging.info("All dimensional tables saved successfully")
