# Diabetes Health Indicators ETL Pipeline

## Introduction

This project implements a **complete ETL pipeline** for analyzing health indicators related to diabetes, using data from the **BRFSS 2015** (Behavioral Risk Factor Surveillance System). The system processes and transforms the data into a **dimensional model (Star Schema)** optimized for analytical queries and efficient reporting.

### Project Objectives

* **Extraction** of raw data from BRFSS 2015
* **Transformation** and cleaning of data with robust validations
* **Dimensional modeling** to optimize analytical queries
* **Loading** into a MySQL database with a star schema
* **Correlation analysis** between health-related variables

### System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   EXTRACTION    │    │  TRANSFORMATION │    │   DIMENSIONAL   │    │     LOADING     │
│                 │    │                 │    │    MODELING     │    │                 │
│ • Raw CSV       │───▶│ • Data Cleaning │───▶│ • Star Schema   │───▶│ • MySQL Tables │
│ • Feature       │    │ • Validation    │    │ • 4 Dimensions  │    │ • Fact Table    │
│   Selection     │    │ • Mapping       │    │ • 1 Fact Table  │    │ • Indexes       │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Model

**Dimension Tables:**

* `dim_demographics`: Demographic information (gender, age, education, income)
* `dim_lifestyle`: Lifestyle factors (smoking, physical activity, diet)
* `dim_medical_conditions`: Medical conditions (blood pressure, cholesterol, diseases)
* `dim_healthcare_access`: Healthcare access (coverage, costs)

**Fact Table:**

* `fact_health_records`: Main metrics + foreign keys to dimensions

## Installation and Setup

### 1. Prerequisites

* Python 3.8+
* MySQL Server 8.0+
* Git

### 2. Clone the Repository

```bash
git clone https://github.com/JuanHoyos329/ODS_3_Diabetes.git
cd ODS_3_Diabetes
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

⚠️ **Important:**
If the CSV file is missing after cloning, you must download it manually from this Google Drive link and place it inside `data/raw/`:

👉 [Download CSV](https://drive.google.com/file/d/1arJI0too-0EQAlofRKzm1RUhBnj1aFIR/view?usp=drive_link)

---

### Database Connection Setup

Edit the `config.py` file at the project root:

```python
# MySQL Database Configuration
DB_CONFIG = {
    "host": "localhost",        
    "user": "user",         # CHANGE: Your MySQL username
    "password": "password",# CHANGE: Your MySQL password
    "database": "diabetesDB",   
    "port": 3306,               
}
```

## How to Run the Pipeline

### Full Execution (Recommended)

```bash
python main.py
```

## Project Structure

```
Proyecto-ETL/
├── data/                    # Project data
│   ├── raw/                 # Raw data (CSV file goes here)
├── logs/                    # Log files
├── src/                     # Source code
│   ├── extraction.py        # Data extraction
│   ├── transform.py         # Data transformation and cleaning
│   ├── dimensional_etl.py   # Dimensional modeling
│   ├── load.py              # Database loading
│   └── utils.py             # Utility functions
├── main.py                  # Main ETL script
├── config.py                # Project configurations
├── requirements.txt         # Python dependencies
└── README.md
```

## Detailed Data Flow

### 1. Extraction (`extraction.py`)

* Loads the BRFSS 2015 CSV file
* Selects 22 features relevant to diabetes
* Handles file-not-found errors
* Supports test sampling

### 2. Transformation (`transform.py`)

* **Cleaning**: Removes missing values and outliers
* **Variable Mapping**:

  * DIABETE3: 1 → "Diabetic", 2 → "Healthy", 3 → "Prediabetic"
  * Binary variables: 1 → "Yes", 2 → "No"
  * SEX: 1 → "Male", 2 → "Female"
* **Validation**: Ensures valid ranges
* **Normalization**: Adjusts BMI (divided by 100)

### 3. Dimensional Modeling (`dimensional_etl.py`)

* **Normalization**: Extracts unique combinations into dimensions
* **Denormalization**: Keeps metrics in fact table
* **Primary Keys**: Auto-generates IDs
* **Reference Mapping**: Connects facts to dimensions

### 4. Loading (`load.py`)

* **Schema Creation**: Optimized tables with indexes
* **Batch Insertion**: Efficient loading for large volumes
* **Validation**: Checks record counts
* **Error Handling**: Rollback on failure

---

*Last updated: September 2025*
