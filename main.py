import sys
import os
import logging
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import extraction functions
from extraction import (
    load_child_mortality_data,
    extract_socioeconomic_data_from_api,
    extract_colombian_socioeconomic_data
)

# Import transformation functions
from transform import full_transformation_pipeline_mortality

# Import dimensional modeling
from dimensional_etl import (
    dimensional_model_mortality,
    save_dimensional_tables
)

# Import loading functions
from load import MySQLLoaderMortality

# Import configuration
from config import DB_CONFIG_MORTALITY


def setup_logging():
    """
    Configure logging for the ETL pipeline.
    """
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Create log filename with timestamp
    log_filename = f"logs/mortality_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logging.info(f"Logging initialized: {log_filename}")
    return log_filename


def print_section_header(title: str):
    """
    Print a formatted section header.
    
    Args:
        title: Section title
    """
    print("\n" + "=" * 70)
    print(title.center(70))
    print("=" * 70)


def main():
    """
    Main ETL pipeline execution.
    """
    # Setup logging
    log_file = setup_logging()
    
    print_section_header("CHILD MORTALITY ETL PIPELINE")
    print(f"\n📋 Log file: {log_file}")
    
    try:
        # ============================================================
        # STEP 1: EXTRACTION - Load Child Mortality Data from CSV
        # ============================================================
        print_section_header("STEP 1: EXTRACTING CHILD MORTALITY DATA")
        
        mortality_file = "data/raw/child_mortality_indicators_col.csv"
        print(f"\n📂 Loading data from: {mortality_file}")
        
        df_mortality_raw = load_child_mortality_data(mortality_file)
        
        print(f"✓ Mortality data loaded: {df_mortality_raw.shape[0]:,} rows, {df_mortality_raw.shape[1]} columns")
        print(f"\nSample columns: {list(df_mortality_raw.columns)[:5]}...")
        
        # ============================================================
        # STEP 2: EXTRACTION - Query Socioeconomic Data from API
        # ============================================================
        print_section_header("STEP 2: EXTRACTING SOCIOECONOMIC DATA FROM API")
        
        print("\n🌐 Querying World Bank API for socioeconomic indicators...")
        print("   (This may take a moment...)")
        
        # Extract years from mortality data
        if 'YEAR (DISPLAY)' in df_mortality_raw.columns:
            years_in_data = sorted(df_mortality_raw['YEAR (DISPLAY)'].dropna().astype(int).unique())
            print(f"\n   Year range in mortality data: {min(years_in_data)} - {max(years_in_data)}")
            
            # Query API for these years
            df_socioeconomic_raw = extract_socioeconomic_data_from_api(
                start_year=min(years_in_data),
                end_year=max(years_in_data)
            )
        else:
            # Default year range
            df_socioeconomic_raw = extract_socioeconomic_data_from_api()
        
        if not df_socioeconomic_raw.empty:
            print(f"✓ Socioeconomic data retrieved: {df_socioeconomic_raw.shape[0]:,} rows")
            # Los datos ya están pivoteados, contar columnas (excluyendo 'year')
            print(f"   Indicators: {len(df_socioeconomic_raw.columns) - 1}")
        else:
            print("⚠️  No socioeconomic data retrieved from API")
            print("   Pipeline will continue with mortality data only")
        
        # Optional: Query Colombian specific APIs
        print("\n📊 Colombian API Integration:")
        print("   See logs for information on integrating Colombian data sources")
        df_colombian = extract_colombian_socioeconomic_data()
        
        # ============================================================
        # STEP 3: TRANSFORMATION - Clean and Merge Data
        # ============================================================
        print_section_header("STEP 3: TRANSFORMING AND MERGING DATA")
        
        print("\n🔄 Running transformation pipeline...")
        print("   - Cleaning mortality data")
        print("   - Cleaning socioeconomic data")
        print("   - Aggregating indicators")
        print("   - Merging datasets on year")
        print("   - Handling missing values")
        
        df_clean = full_transformation_pipeline_mortality(
            df_mortality_raw,
            df_socioeconomic_raw,
            missing_value_strategy='interpolate'  # Imputación inteligente con interpolación
        )
        
        print(f"\n✓ Transformation completed")
        print(f"   Final dataset: {df_clean.shape[0]:,} rows, {df_clean.shape[1]} columns")
        
        if 'year' in df_clean.columns:
            print(f"   Year range: {df_clean['year'].min()} - {df_clean['year'].max()}")
        
        # Save intermediate result
        output_file = "data/processed/child_mortality_integrated.csv"
        os.makedirs("data/processed", exist_ok=True)
        df_clean.to_csv(output_file, index=False)
        print(f"\n💾 Saved integrated data to: {output_file}")
        
        # ============================================================
        # STEP 4: DIMENSIONAL MODELING
        # ============================================================
        print_section_header("STEP 4: CREATING DIMENSIONAL MODEL")
        
        print("\n🏗️  Building star schema...")
        print("   - Time dimension")
        print("   - Mortality indicators dimension")
        print("   - Socioeconomic indicators dimension")
        print("   - Fact table (child health)")
        
        tables = dimensional_model_mortality(df_clean)
        
        print(f"\n✓ Dimensional model created:")
        for table_name, df_table in tables.items():
            print(f"   - {table_name}: {len(df_table):,} records")
        
        # Save dimensional tables to CSV
        save_dimensional_tables(tables, "data/processed")
        print(f"\n💾 Dimensional tables saved to: data/processed/")
        
        # ============================================================
        # STEP 5: LOADING TO DATABASE
        # ============================================================
        print_section_header("STEP 5: LOADING DATA TO MYSQL")
        
        print(f"\n🗄️  Database: {DB_CONFIG_MORTALITY['database']}")
        print(f"   Host: {DB_CONFIG_MORTALITY['host']}:{DB_CONFIG_MORTALITY['port']}")
        
        # Initialize loader
        loader = MySQLLoaderMortality(**DB_CONFIG_MORTALITY)
        
        # Create database
        print("\n   Creating database...")
        if not loader.create_database():
            raise Exception("Failed to create database")
        
        # Connect to database
        print("   Connecting to database...")
        if not loader.connect():
            raise Exception("Failed to connect to database")
        
        # Create tables
        print("   Creating tables...")
        if not loader.create_all_tables():
            raise Exception("Failed to create tables")
        
        # Load data
        print("   Loading dimensional tables...")
        loader.load_dimensional_tables(tables)
        
        # Verify data load
        print("\n   Verifying data load...")
        counts = loader.verify_data_load()
        
        # Close connection
        loader.disconnect()
        
        print("\n✓ Data loaded successfully to MySQL")
        
        # ============================================================
        # PIPELINE COMPLETED
        # ============================================================
        print_section_header("ETL PIPELINE COMPLETED SUCCESSFULLY!")
        
        print("\n📊 Summary:")
        print(f"   ✓ Mortality records processed: {df_mortality_raw.shape[0]:,}")
        print(f"   ✓ Socioeconomic records retrieved: {df_socioeconomic_raw.shape[0]:,}")
        print(f"   ✓ Final integrated records: {df_clean.shape[0]:,}")
        print(f"   ✓ Database tables created: {len(tables)}")
        print(f"   ✓ Total records in database: {sum(counts.values()):,}")
        
        print("\n📁 Output Files:")
        print(f"   - Integrated data: {output_file}")
        print(f"   - Dimensional tables: data/processed/*.csv")
        print(f"   - Log file: {log_file}")
        
        print("\n🔍 Next Steps:")
        print("   1. Query the MySQL database for analysis")
        print("   2. Create visualizations and dashboards")
        print("   3. Investigate relationships between mortality and socioeconomic factors")
        
        print("\n💡 Sample Query:")
        print("   SELECT year, infant_mortality_rate, gdp_per_capita")
        print("   FROM fact_child_health f")
        print("   JOIN dim_time t ON f.time_id = t.time_id")
        print("   JOIN dim_socioeconomic_indicators s ON f.socioeconomic_indicator_id = s.socioeconomic_indicator_id")
        print("   ORDER BY year;")
        
        return 0
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: File not found - {e}")
        logging.error(f"File not found: {e}")
        return 1
        
    except Exception as e:
        print(f"\n❌ ERROR: Pipeline failed - {e}")
        logging.error(f"Pipeline error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
