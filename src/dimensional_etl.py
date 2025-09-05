import pandas as pd
import os
import sys
import logging
from datetime import datetime

from utils import setup_logging, ensure_directory_exists

def load_diabetes_dataset(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Dataset not found at: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Successfully loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        logging.error(f"Error loading dataset: {str(e)}")
        raise

def create_dim_demographics(df: pd.DataFrame) -> pd.DataFrame:
    demo_cols = ['Sex', 'Age', 'Education', 'Income']
    demo_df = df[demo_cols].drop_duplicates().reset_index(drop=True)
    demo_df['demographic_id'] = range(1, len(demo_df) + 1)
    return demo_df

def create_dim_lifestyle(df: pd.DataFrame) -> pd.DataFrame:
    lifestyle_cols = ['Smoker', 'PhysActivity', 'Fruits', 'Veggies', 'HvyAlcoholConsump']
    lifestyle_df = df[lifestyle_cols].drop_duplicates().reset_index(drop=True)
    lifestyle_df['lifestyle_id'] = range(1, len(lifestyle_df) + 1)
    return lifestyle_df

def create_dim_medical_conditions(df: pd.DataFrame) -> pd.DataFrame:
    medical_cols = ['HighBP', 'HighChol', 'Stroke', 'HeartDiseaseorAttack']
    medical_df = df[medical_cols].drop_duplicates().reset_index(drop=True)
    medical_df['medical_conditions_id'] = range(1, len(medical_df) + 1)
    return medical_df

def create_dim_healthcare_access(df: pd.DataFrame) -> pd.DataFrame:
    healthcare_cols = ['AnyHealthcare', 'NoDocbcCost', 'CholCheck']
    healthcare_df = df[healthcare_cols].drop_duplicates().reset_index(drop=True)
    healthcare_df['healthcare_access_id'] = range(1, len(healthcare_df) + 1)
    return healthcare_df

def create_fact_health_records(df: pd.DataFrame, dim_demographics: pd.DataFrame, 
                             dim_lifestyle: pd.DataFrame, dim_medical_conditions: pd.DataFrame, 
                             dim_healthcare_access: pd.DataFrame) -> pd.DataFrame:
    # Create mapping for demographics
    demo_cols = ['Sex', 'Age', 'Education', 'Income']
    demo_mapping = df[demo_cols].merge(dim_demographics, on=demo_cols, how='left')['demographic_id']
    
    # Create mapping for lifestyle
    lifestyle_cols = ['Smoker', 'PhysActivity', 'Fruits', 'Veggies', 'HvyAlcoholConsump']
    lifestyle_mapping = df[lifestyle_cols].merge(dim_lifestyle, on=lifestyle_cols, how='left')['lifestyle_id']
    
    # Create mapping for medical conditions
    medical_cols = ['HighBP', 'HighChol', 'Stroke', 'HeartDiseaseorAttack']
    medical_mapping = df[medical_cols].merge(dim_medical_conditions, on=medical_cols, how='left')['medical_conditions_id']
    
    # Create mapping for healthcare access
    healthcare_cols = ['AnyHealthcare', 'NoDocbcCost', 'CholCheck']
    healthcare_mapping = df[healthcare_cols].merge(dim_healthcare_access, on=healthcare_cols, how='left')['healthcare_access_id']
    
    # Create fact table
    fact_df = pd.DataFrame({
        'record_id': range(1, len(df) + 1),
        'diabetes_status': df['diabetes_status'],
        'bmi_value': df['BMI'],
        'mental_health_days': df['MentHlth'],
        'physical_health_days': df['PhysHlth'],
        'general_health_score': df['GenHlth'],
        'difficulty_walking': df['DiffWalk'],
        'demographic_id': demo_mapping.values,
        'lifestyle_id': lifestyle_mapping.values,
        'medical_conditions_id': medical_mapping.values,
        'healthcare_access_id': healthcare_mapping.values
    })
    
    return fact_df

def save_dimensional_tables(output_dir: str, dim_demographics: pd.DataFrame, 
                          dim_lifestyle: pd.DataFrame, dim_medical_conditions: pd.DataFrame,
                          dim_healthcare_access: pd.DataFrame, fact_health_records: pd.DataFrame):
    ensure_directory_exists(output_dir)
    
    tables = {
        'dim_demographics.csv': dim_demographics,
        'dim_lifestyle.csv': dim_lifestyle,
        'dim_medical_conditions.csv': dim_medical_conditions,
        'dim_healthcare_access.csv': dim_healthcare_access,
        'fact_health_records.csv': fact_health_records
    }
    
    for filename, df in tables.items():
        filepath = os.path.join(output_dir, filename)
        df.to_csv(filepath, index=False)
        logging.info(f"Saved {filename}: {len(df)} records")
        print(f"Saved {filename}")

def display_sample_data(dim_demographics: pd.DataFrame, dim_lifestyle: pd.DataFrame,
                       dim_medical_conditions: pd.DataFrame, dim_healthcare_access: pd.DataFrame,
                       fact_health_records: pd.DataFrame):
    print(f"\nSample Data from Dimensional Tables:")
    
    print(f"\nDemographics (showing first 3 records):")
    for _, row in dim_demographics.head(3).iterrows():
        print(f"     {dict(row)}")
    
    print(f"\nLifestyle (showing first 3 records):")
    for _, row in dim_lifestyle.head(3).iterrows():
        print(f"     {dict(row)}")
    
    print(f"\nMedical Conditions (showing first 3 records):")
    for _, row in dim_medical_conditions.head(3).iterrows():
        print(f"     {dict(row)}")
    
    print(f"\nHealthcare Access (showing first 3 records):")
    for _, row in dim_healthcare_access.head(3).iterrows():
        print(f"     {dict(row)}")
    
    print(f"\nData Warehouse Summary:")
    print(f"   Total Fact Records: {len(fact_health_records):,}")
    total_dim_records = len(dim_demographics) + len(dim_lifestyle) + len(dim_medical_conditions) + len(dim_healthcare_access)
    print(f"   Total Dimension Records: {total_dim_records:,}")
    print(f"   Compression Ratio: {len(fact_health_records) / (len(fact_health_records) + total_dim_records):.1%} facts vs dimensions")

def dimensional_model(df_clean):
    logging.info("Creating dimensional model from clean DataFrame")
    
    # Create Demographics Dimension
    demo_cols = ['Sex', 'Age', 'Education', 'Income']
    demo_df = df_clean[demo_cols].drop_duplicates().reset_index(drop=True)
    demo_df['demographic_id'] = range(1, len(demo_df) + 1)
    demo_df.rename(columns={
        'Sex': 'sex',
        'Age': 'age_group', 
        'Education': 'education_level',
        'Income': 'income_bracket'
    }, inplace=True)
    
    # Create Lifestyle Dimension
    lifestyle_cols = ['Smoker', 'PhysActivity', 'Fruits', 'Veggies', 'HvyAlcoholConsump']
    lifestyle_df = df_clean[lifestyle_cols].drop_duplicates().reset_index(drop=True)
    lifestyle_df['lifestyle_id'] = range(1, len(lifestyle_df) + 1)
    lifestyle_df.rename(columns={
        'Smoker': 'smoker_status',
        'PhysActivity': 'physical_activity',
        'Fruits': 'fruits_consumption',
        'Veggies': 'vegetables_consumption', 
        'HvyAlcoholConsump': 'heavy_alcohol_consumption'
    }, inplace=True)
    
    # Create Medical Conditions Dimension
    medical_cols = ['HighBP', 'HighChol', 'CholCheck', 'Stroke', 'HeartDiseaseorAttack', 'DiffWalk']
    medical_df = df_clean[medical_cols].drop_duplicates().reset_index(drop=True)
    medical_df['medical_conditions_id'] = range(1, len(medical_df) + 1)
    medical_df.rename(columns={
        'HighBP': 'high_blood_pressure',
        'HighChol': 'high_cholesterol',
        'CholCheck': 'cholesterol_check',
        'Stroke': 'stroke_history',
        'HeartDiseaseorAttack': 'heart_disease_or_attack',
        'DiffWalk': 'difficulty_walking'
    }, inplace=True)
    
    # Create Healthcare Access Dimension  
    healthcare_cols = ['AnyHealthcare', 'NoDocbcCost']
    healthcare_df = df_clean[healthcare_cols].drop_duplicates().reset_index(drop=True)
    healthcare_df['healthcare_access_id'] = range(1, len(healthcare_df) + 1)
    healthcare_df.rename(columns={
        'AnyHealthcare': 'any_healthcare_coverage',
        'NoDocbcCost': 'no_doctor_due_to_cost'
    }, inplace=True)
    
    # Create mapping dictionaries for fact table
    # Demographics mapping
    demo_mapping_dict = {}
    for idx, row in demo_df.iterrows():
        key = (row['sex'], row['age_group'], row['education_level'], row['income_bracket'])
        demo_mapping_dict[key] = row['demographic_id']
    
    # Apply demographics mapping
    demo_ids = []
    for idx, row in df_clean.iterrows():
        key = (row['Sex'], row['Age'], row['Education'], row['Income'])
        demo_ids.append(demo_mapping_dict[key])
    
    # Lifestyle mapping
    lifestyle_mapping_dict = {}
    for idx, row in lifestyle_df.iterrows():
        key = (row['smoker_status'], row['physical_activity'], row['fruits_consumption'], 
               row['vegetables_consumption'], row['heavy_alcohol_consumption'])
        lifestyle_mapping_dict[key] = row['lifestyle_id']
    
    # Apply lifestyle mapping
    lifestyle_ids = []
    for idx, row in df_clean.iterrows():
        key = (row['Smoker'], row['PhysActivity'], row['Fruits'], row['Veggies'], row['HvyAlcoholConsump'])
        lifestyle_ids.append(lifestyle_mapping_dict[key])
    
    # Medical conditions mapping
    medical_mapping_dict = {}
    for idx, row in medical_df.iterrows():
        key = (row['high_blood_pressure'], row['high_cholesterol'], row['cholesterol_check'], 
               row['stroke_history'], row['heart_disease_or_attack'], row['difficulty_walking'])
        medical_mapping_dict[key] = row['medical_conditions_id']
    
    # Apply medical mapping
    medical_ids = []
    for idx, row in df_clean.iterrows():
        key = (row['HighBP'], row['HighChol'], row['CholCheck'], row['Stroke'], row['HeartDiseaseorAttack'], row['DiffWalk'])
        medical_ids.append(medical_mapping_dict[key])
    
    # Healthcare access mapping
    healthcare_mapping_dict = {}
    for idx, row in healthcare_df.iterrows():
        key = (row['any_healthcare_coverage'], row['no_doctor_due_to_cost'])
        healthcare_mapping_dict[key] = row['healthcare_access_id']
    
    # Apply healthcare mapping
    healthcare_ids = []
    for idx, row in df_clean.iterrows():
        key = (row['AnyHealthcare'], row['NoDocbcCost'])
        healthcare_ids.append(healthcare_mapping_dict[key])
    
    # Create Fact Table
    fact_df = pd.DataFrame({
        'record_id': range(1, len(df_clean) + 1),
        'diabetes_status': df_clean['diabetes_status'].astype(str),
        'bmi_value': df_clean['BMI'].astype(float),
        'mental_health_days': df_clean['MentHlth'].astype(float), 
        'physical_health_days': df_clean['PhysHlth'].astype(float),
        'general_health_score': df_clean['GenHlth'].astype(float),
        'demographic_id': demo_ids,
        'lifestyle_id': lifestyle_ids,
        'medical_conditions_id': medical_ids,
        'healthcare_access_id': healthcare_ids
    })
    
    tables = {
        'dim_demographics': demo_df,
        'dim_lifestyle': lifestyle_df,
        'dim_medical_conditions': medical_df,
        'dim_healthcare_access': healthcare_df,
        'fact_health_records': fact_df
    }
    
    logging.info(f"Created dimensional model with {len(tables)} tables")
    for table_name, table_df in tables.items():
        logging.info(f"  - {table_name}: {len(table_df)} records")
    
    return tables

def main():
    print("=" * 60)
    print("DIMENSIONAL ETL - DIABETES HEALTH INDICATORS")
    print("=" * 60)
    
    # Setup logging
    log_dir = 'logs'
    ensure_directory_exists(log_dir)
    log_file = os.path.join(log_dir, f'dimensional_etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    setup_logging('INFO', log_file)
    
    logging.info("Starting Dimensional ETL process")
    print(f"Logging to: {log_file}")
    
    try:
        # Load the dataset
        data_file = os.path.join('..', 'data', 'processed', 'diabetes_012_processed.csv')
        df = load_diabetes_dataset(data_file)
        
        print(f"\n📊 Dataset Info:")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        
        # Create dimensional tables
        print(f"\nCreating Dimensional Tables...")
        
        dim_demographics = create_dim_demographics(df)
        print(f"Demographics: {len(dim_demographics)} unique combinations")
        
        dim_lifestyle = create_dim_lifestyle(df)
        print(f"Lifestyle: {len(dim_lifestyle)} unique combinations")
        
        dim_medical_conditions = create_dim_medical_conditions(df)
        print(f"Medical Conditions: {len(dim_medical_conditions)} unique combinations")
        
        dim_healthcare_access = create_dim_healthcare_access(df)
        print(f"Healthcare Access: {len(dim_healthcare_access)} unique combinations")
        
        # Create fact table
        print(f"\nCreating Fact Table...")
        fact_health_records = create_fact_health_records(df, dim_demographics, dim_lifestyle,
                                                       dim_medical_conditions, dim_healthcare_access)
        print(f"Fact Health Records: {len(fact_health_records)} records")
        
        # Save dimensional tables
        print(f"\nSaving Dimensional Tables...")
        output_dir = os.path.join('..', 'data', 'processed')
        save_dimensional_tables(output_dir, dim_demographics, dim_lifestyle, 
                               dim_medical_conditions, dim_healthcare_access, fact_health_records)
        
        # Display sample data
        display_sample_data(dim_demographics, dim_lifestyle, dim_medical_conditions,
                          dim_healthcare_access, fact_health_records)
        
        print(f"\n" + "=" * 60)
        print("DIMENSIONAL ETL COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        print(f"\nOutput Files:")
        print(f"   - {output_dir}/dim_demographics.csv ({len(dim_demographics)} records)")
        print(f"   - {output_dir}/dim_lifestyle.csv ({len(dim_lifestyle)} records)")
        print(f"   - {output_dir}/dim_medical_conditions.csv ({len(dim_medical_conditions)} records)")
        print(f"   - {output_dir}/dim_healthcare_access.csv ({len(dim_healthcare_access)} records)")
        print(f"   - {output_dir}/fact_health_records.csv ({len(fact_health_records)} records)")
        
        logging.info("Dimensional ETL completed successfully")
        
    except Exception as e:
        error_msg = f"Error during dimensional ETL: {str(e)}"
        print(f"\n{error_msg}")
        logging.error(error_msg, exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()