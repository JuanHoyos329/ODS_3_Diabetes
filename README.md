# Diabetes Health Indicators ETL Pipeline

A complete ETL pipeline for processing BRFSS 2015 Diabetes Health Indicators dataset from raw data directly to MySQL database with comprehensive data analysis and visualizations.

## Project Structure

```
├── data/
│   └── raw/                      # Raw BRFSS 2015 data files
├── src/
│   ├── data_extraction.py        # Raw data extraction functions
│   ├── data_transformation.py    # Data transformation pipeline
│   ├── dimensional_etl.py        # Dimensional modeling (Star Schema)
│   ├── database_loader.py        # MySQL database loader
│   ├── config.py                 # Database configuration
│   └── utils.py                 # Utility functions
├── logs/                        # Pipeline execution logs
├── sql/                         # SQL scripts and queries
├── main.py                      # Complete ETL pipeline (Raw → MySQL)
├── requirements.txt             # Python dependencies
└── README.md                   # This file
```

## Dataset Information

**441,456 raw BRFSS 2015 survey responses → 292,140 clean records**

This pipeline processes the complete Behavioral Risk Factor Surveillance System (BRFSS) 2015 dataset from raw format directly into a MySQL data warehouse with dimensional modeling and comprehensive analysis.

**Final Dataset Classification**:
- **0**: No diabetes (83.1% - 242,627 records)
- **1**: Pre-diabetes (0.8% - 2,246 records)  
- **2**: Diabetes (14.2% - 41,566 records)

## Key Features

The pipeline processes 22 health indicator features:

### Demographics
- **Sex**: Gender (1=Male, 2=Female)
- **Age**: Age groups (1-13, representing 5-year increments)
- **Education**: Education level (1-6)
- **Income**: Household income (1-8)

### Medical Conditions
- **HighBP**: High blood pressure
- **HighChol**: High cholesterol  
- **Stroke**: Ever had stroke
- **HeartDiseaseorAttack**: Coronary heart disease/MI

### Lifestyle Factors
- **Smoker**: Smoked 100+ cigarettes
- **PhysActivity**: Physical activity in past 30 days
- **Fruits**: Consume fruits 1+ times per day
- **Veggies**: Consume vegetables 1+ times per day
- **HvyAlcoholConsump**: Heavy alcohol consumption

### Healthcare Access
- **AnyHealthcare**: Has healthcare coverage
- **NoDocbcCost**: Could not see doctor due to cost
- **CholCheck**: Cholesterol check in past 5 years

### Health Metrics
- **GenHlth**: General health (1=Excellent to 5=Poor)
- **MentHlth**: Mental health days (0-30)
- **PhysHlth**: Physical health days (0-30)
- **DiffWalk**: Difficulty walking/climbing stairs
- **BMI**: Body Mass Index

## Prerequisites

### MySQL Database Setup
1. **Install MySQL Server 8.0+**
2. **Create database**:
```sql
CREATE DATABASE diabetesDB;
```
3. **Update config.py** with your MySQL settings:
```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '',  # Your password
    'database': 'diabetesDB'
}
```

### Raw Data Setup
Place the raw BRFSS 2015 dataset as:
```
data/raw/diabetes_012_health_indicators_BRFSS2015.csv
```

## Installation

1. **Clone repository**:
```bash
git clone <repository-url>
cd "Proyecto ETL"
```

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

## Usage

### Complete ETL Pipeline
Run the full pipeline from raw data to MySQL with analysis and visualizations:

```bash
python3 main.py
```

### Get Help
```bash
python3 main.py --help
```

## ETL Pipeline Process

The `main.py` script executes a complete 6-step ETL process:

### Step 1: Data Extraction
- Loads raw BRFSS 2015 dataset (441,456 records)
- Selects 22 diabetes-related features
- Initial data validation

### Step 2: Data Transformation
- Removes missing values (~149K rows removed)
- Transforms diabetes variable (DIABETE3 → 0,1,2)
- Converts binary variables to 0/1 format
- Scales BMI values (/100)
- Cleans ordinal variables (removes invalid responses)
- Standardizes column names

### Step 3: Dimensional Modeling
Creates star schema with 4 dimension tables + 1 fact table:
- **dim_demographics**: 1,148 unique demographic combinations
- **dim_lifestyle**: 245 unique lifestyle combinations  
- **dim_medical_conditions**: 311 unique medical condition combinations
- **dim_healthcare_access**: 14 unique healthcare access combinations
- **fact_health_records**: 292,140 health records with metrics + foreign keys

### Step 4: Database Connection
- Connects to MySQL database (diabetesDB)
- Validates connection and permissions

### Step 5: Data Loading
- Creates dimensional tables with proper schema
- Creates fact table with foreign key constraints
- Loads all data to MySQL with validation
- Total: 292,140 fact records + 1,718 dimension records

### Step 6: Data Analysis & Visualizations
Generates comprehensive analysis with visualizations:

#### Generated Charts
- **diabetes_distribution.png**: Diabetes status distribution (pie + bar charts)
- **age_group_analysis.png**: Diabetes rates by age group (1.5% → 22.7%)
- **lifestyle_analysis.png**: Physical activity, smoking, and BMI impact analysis

#### Key Insights
- **Age Impact**: Diabetes rates increase dramatically with age (1.5% young → 22.7% older)
- **Lifestyle Impact**: Physical activity reduces diabetes rate (21.9% inactive vs 12.1% active)
- **Population Health**: 14.2% diabetes prevalence in cleaned dataset

## MySQL Database Schema

### Dimension Tables
```sql
dim_demographics      -- 1,148 records (sex, age_group, education_level, income_bracket)
dim_lifestyle        -- 245 records (smoker_status, physical_activity, fruits/veggies consumption)
dim_medical_conditions -- 311 records (blood pressure, cholesterol, stroke, heart disease)
dim_healthcare_access -- 14 records (healthcare coverage, cost barriers)
```

### Fact Table
```sql
fact_health_records  -- 292,140 records (diabetes_status, BMI, health days + dimension keys)
```

### Key Benefits
- **Normalized Design**: Eliminates data redundancy
- **Query Performance**: Optimized for analytical queries
- **Data Integrity**: Foreign key constraints ensure consistency
- **Analytics Ready**: Perfect for BI tools and dashboards

## Output Files

After pipeline execution:

### Database
- Complete star schema loaded in MySQL `diabetesDB`
- Ready for analytical queries and reporting

### Visualizations
```
diabetes_distribution.png    # Diabetes status distribution charts
age_group_analysis.png      # Age group diabetes rate progression  
lifestyle_analysis.png      # Lifestyle factors impact analysis
```

### Logs
```
logs/diabetes_analysis_YYYYMMDD_HHMMSS.log  # Complete execution log
```

## Data Quality & Validation

The pipeline ensures high data quality through:
- **Missing Value Removal**: Complete cases only (292K from 441K)
- **Invalid Response Cleaning**: Removes "Don't know/Refused" responses
- **Data Type Standardization**: Consistent 0/1 binary encoding
- **Referential Integrity**: Foreign key constraints in MySQL
- **Automated Validation**: Built-in data quality checks

## Sample MySQL Queries

```sql
-- Diabetes distribution summary
SELECT diabetes_status, COUNT(*) as count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
FROM fact_health_records 
GROUP BY diabetes_status;

-- Age group analysis
SELECT d.age_group, 
       AVG(CASE WHEN f.diabetes_status = 2 THEN 1.0 ELSE 0.0 END) * 100 as diabetes_rate,
       COUNT(*) as total_records
FROM fact_health_records f
JOIN dim_demographics d ON f.demographic_id = d.demographic_id
GROUP BY d.age_group
ORDER BY d.age_group;

-- Lifestyle impact analysis  
SELECT l.physical_activity,
       AVG(f.bmi_value) as avg_bmi,
       AVG(CASE WHEN f.diabetes_status = 2 THEN 1.0 ELSE 0.0 END) * 100 as diabetes_rate
FROM fact_health_records f
JOIN dim_lifestyle l ON f.lifestyle_id = l.lifestyle_id
GROUP BY l.physical_activity;
```

## Dependencies

All required packages listed in `requirements.txt`:
- `pandas` - Data manipulation and analysis
- `numpy` - Numerical computing
- `matplotlib` - Visualization and plotting
- `seaborn` - Statistical data visualization
- `mysql-connector-python` - MySQL database connectivity

## License

This project processes public health surveillance data from the CDC's Behavioral Risk Factor Surveillance System (BRFSS).

## References

- [BRFSS 2015 Codebook](https://www.cdc.gov/brfss/annual_data/2015/pdf/codebook15_llcp.pdf)
- [CDC Diabetes Risk Factors Research](https://www.cdc.gov/pcd/issues/2019/19_0109.htm)
- CDC Behavioral Risk Factor Surveillance System