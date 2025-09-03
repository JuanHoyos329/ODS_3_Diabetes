import pandas as pd
import sys
sys.path.append('src')

from extraction import load_raw_data
from transform import full_transformation_pipeline

print("=== DEBUGGING DATA TRANSFORMATION ===")

# 1. Cargar datos sin procesar
print("\n1. Cargando datos originales...")
try:
    df_raw = load_raw_data(load_all_years=False)
    print(f"Datos cargados: {df_raw.shape}")
    
    # Mostrar valores únicos en columnas problemáticas
    binary_columns = [
        '_RFHYPE5', 'TOLDHI2', '_CHOLCHK', 'SMOKE100', 'CVDSTRK3',
        '_MICHD', '_TOTINDA', '_FRTLT1', '_VEGLT1', '_RFDRHV5',
        'HLTHPLN1', 'MEDCOST', 'DIFFWALK'
    ]
    
    print("\n2. Valores únicos en columnas binarias ANTES de transformación:")
    for col in binary_columns:
        if col in df_raw.columns:
            unique_vals = sorted(df_raw[col].dropna().unique())
            print(f"   {col}: {unique_vals}")
    
    # 3. Aplicar transformación
    print("\n3. Aplicando transformación...")
    df_clean = full_transformation_pipeline(df_raw.copy())
    
    # Mapear nombres de columnas después de transformación
    col_mapping = {
        '_RFHYPE5': 'HighBP',
        'TOLDHI2': 'HighChol',
        '_CHOLCHK': 'CholCheck',
        'SMOKE100': 'Smoker',
        'CVDSTRK3': 'Stroke',
        '_MICHD': 'HeartDiseaseorAttack',
        '_TOTINDA': 'PhysActivity',
        '_FRTLT1': 'Fruits',
        '_VEGLT1': 'Veggies',
        '_RFDRHV5': 'HvyAlcoholConsump',
        'HLTHPLN1': 'AnyHealthcare',
        'MEDCOST': 'NoDocbcCost',
        'DIFFWALK': 'DiffWalk'
    }
    
    print("\n4. Valores únicos en columnas binarias DESPUÉS de transformación:")
    for old_col, new_col in col_mapping.items():
        if new_col in df_clean.columns:
            unique_vals = sorted(df_clean[new_col].dropna().unique())
            print(f"   {new_col}: {unique_vals}")
            
            # Verificar si hay valores problemáticos
            if any(val not in [0, 1] for val in unique_vals):
                print(f"   ❌ PROBLEMA: {new_col} tiene valores que no son 0 o 1!")
                
    print(f"\n5. Dataset final: {df_clean.shape}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
