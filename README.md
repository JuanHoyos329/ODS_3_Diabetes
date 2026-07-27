# Diabetes Analytics Data Warehouse

## Overview

This project implements an **end-to-end ETL pipeline** for processing health indicators associated with diabetes using the **BRFSS 2015 (Behavioral Risk Factor Surveillance System)** dataset.

The solution extracts raw healthcare data, applies data quality validations and transformations, and loads the results into a **dimensional data warehouse (Star Schema)** optimized for analytical queries, reporting, and business intelligence workloads.

The project demonstrates practical skills in **Data Engineering, ETL development, dimensional modeling, and relational database design**.

---

## Key Features

* Automated ETL pipeline using Python
* Data cleaning and validation workflows
* Dimensional modeling with a **Star Schema**
* MySQL data warehouse implementation
* Batch loading with transactional integrity
* Correlation-ready analytical dataset
* Modular and maintainable project structure

---

## Architecture

```text
Raw BRFSS CSV
        │
        ▼
Data Extraction
        │
        ▼
Data Cleaning & Validation
        │
        ▼
Dimensional Modeling
        │
        ▼
MySQL Data Warehouse
        │
        ▼
Analytical Queries & Reporting
```

---

## Dimensional Model

The warehouse follows a **Star Schema** design composed of four dimension tables and one fact table.

### Dimension Tables

| Table                    | Description                                                 |
| ------------------------ | ----------------------------------------------------------- |
| `dim_demographics`       | Gender, age group, education, and income                    |
| `dim_lifestyle`          | Smoking habits, physical activity, and dietary indicators   |
| `dim_medical_conditions` | Blood pressure, cholesterol, and chronic disease indicators |
| `dim_healthcare_access`  | Insurance coverage and healthcare cost barriers             |

### Fact Table

| Table                 | Description                                                                          |
| --------------------- | ------------------------------------------------------------------------------------ |
| `fact_health_records` | Central table containing diabetes-related metrics and foreign keys to all dimensions |

This structure improves **query performance, scalability, and analytical flexibility** compared to a fully normalized transactional model.

---

## Project Structure

```text
Diabetes-Analytics-Data-Warehouse/
├── data/
│   └── raw/
├── logs/
├── src/
│   ├── extraction.py
│   ├── transform.py
│   ├── dimensional_etl.py
│   ├── load.py
│   └── utils.py
├── main.py
├── config.py
├── requirements.txt
└── README.md
```

---

## ETL Workflow

### 1. Extraction

* Load BRFSS 2015 CSV data
* Select diabetes-related features
* Validate file availability
* Support sampling for testing

### 2. Transformation

* Remove invalid and missing records
* Normalize categorical variables
* Validate numerical ranges
* Standardize BMI values
* Apply business rules for diabetes classification

### 3. Dimensional Modeling

* Generate surrogate keys
* Create dimension tables
* Map fact records to dimensions
* Preserve analytical measures in the fact table

### 4. Loading

* Create MySQL schema and indexes
* Perform batch inserts
* Validate record counts
* Execute transactional rollback on failure

---

## Installation

### Requirements

* Python 3.8+
* MySQL Server 8.0+
* Git

### Clone the repository

```bash
git clone https://github.com/JuanHoyos329/ODS_3_Diabetes.git
cd Diabetes-Analytics-Data-Warehouse
```

### Install dependencies

```bash
pip install -r requirements.txt
```

---

## Dataset Setup

Download the BRFSS 2015 dataset and place the CSV file inside:

```text
data/raw/
```

The download link is available in the project documentation.

---

## Database Configuration

Update `config.py` with your MySQL credentials:

```python
DB_CONFIG = {
    "host": "localhost",
    "user": "your_user",
    "password": "your_password",
    "database": "diabetesDB",
    "port": 3306,
}
```

---

## Run the Pipeline

Execute the complete ETL process:

```bash
python main.py
```

The pipeline will:

1. Extract the raw dataset
2. Clean and validate the data
3. Build the dimensional model
4. Load the warehouse into MySQL
5. Validate the final load

---

## Technology Stack

| Category             | Technologies           |
| -------------------- | ---------------------- |
| Programming Language | Python                 |
| Data Processing      | Pandas, NumPy          |
| Database             | MySQL 8                |
| ETL                  | Custom Python Pipeline |
| Modeling             | Star Schema            |
| Version Control      | Git                    |

---

## Engineering Highlights

* **Dimensional Modeling:** Designed a scalable Star Schema for healthcare analytics.
* **Data Quality:** Implemented validation rules for categorical mappings and numerical ranges.
* **Batch Processing:** Optimized loading operations using transactional batch inserts.
* **Modular Design:** Separated extraction, transformation, modeling, and loading responsibilities into independent modules.

---

## Potential Improvements

* Add **Apache Airflow** orchestration.
* Containerize the pipeline with **Docker Compose**.
* Include automated **data quality reports**.
* Expose warehouse metrics through a **BI dashboard** (Power BI or Streamlit).
* Add **unit tests** for ETL transformations.

---

## Author

**Juan Andrés Hoyos Rodríguez**

Data Engineering & Artificial Intelligence

2025
