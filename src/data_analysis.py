# Analysis functions for diabetes data

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List

def generate_summary_statistics(df: pd.DataFrame) -> pd.DataFrame:
    return df.describe()

def analyze_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    missing_data = pd.DataFrame({
        'Column': df.columns,
        'Missing_Count': df.isnull().sum(),
        'Missing_Percentage': (df.isnull().sum() / len(df)) * 100
    })
    return missing_data.sort_values('Missing_Percentage', ascending=False)

def correlation_analysis(df: pd.DataFrame, target_column: str = None) -> pd.DataFrame:
    numeric_df = df.select_dtypes(include=[np.number])
    
    if target_column and target_column in numeric_df.columns:
        return numeric_df.corr()[target_column].sort_values(ascending=False)
    else:
        return numeric_df.corr()

def plot_distribution(df: pd.DataFrame, column: str, save_path: str = None):
    plt.figure(figsize=(10, 6))
    sns.histplot(df[column], kde=True)
    plt.title(f'Distribution of {column}')
    plt.xlabel(column)
    plt.ylabel('Frequency')
    
    if save_path:
        plt.savefig(save_path)
    else:
        plt.show()