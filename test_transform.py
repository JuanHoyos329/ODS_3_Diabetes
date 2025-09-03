import pandas as pd
import sys
import logging
sys.path.append('src')

# Configurar logging para ver todos los mensajes
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from extraction import load_raw_data
from transform import full_transformation_pipeline

print("=== TESTING IMPROVED DATA TRANSFORMATION ===")

# 1. Cargar datos sin procesar
print("\n1. Cargando una muestra de datos originales...")
try:
    df_raw = load_raw_data(load_all_years=False)
    
    # Tomar solo una muestra para testing rápido
    df_sample = df_raw.sample(n=min(10000, len(df_raw)), random_state=42)
    print(f"Muestra de datos: {df_sample.shape}")
    
    # 2. Aplicar transformación mejorada
    print("\n2. Aplicando transformación mejorada...")
    df_clean = full_transformation_pipeline(df_sample.copy())
    
    print(f"\n3. Resultados de la transformación:")
    print(f"   - Filas antes: {len(df_sample)}")
    print(f"   - Filas después: {len(df_clean)}")
    print(f"   - Filas removidas: {len(df_sample) - len(df_clean)}")
    
    # 4. Verificar que todos los valores binarios son 0 o 1
    binary_cols = ['HighBP', 'HighChol', 'CholCheck', 'Smoker', 'Stroke', 
                   'HeartDiseaseorAttack', 'PhysActivity', 'Fruits', 'Veggies', 
                   'HvyAlcoholConsump', 'AnyHealthcare', 'NoDocbcCost', 'DiffWalk']
    
    print(f"\n4. Verificación de columnas binarias:")
    all_valid = True
    for col in binary_cols:
        if col in df_clean.columns:
            unique_vals = sorted(df_clean[col].unique())
            is_valid = all(val in [0, 1] for val in unique_vals)
            status = "✅" if is_valid else "❌"
            print(f"   {status} {col}: {unique_vals}")
            if not is_valid:
                all_valid = False
    
    # 5. Verificar diabetes
    if 'Diabetes_012' in df_clean.columns:
        diabetes_vals = sorted(df_clean['Diabetes_012'].unique())
        is_valid = all(val in [0, 1, 2] for val in diabetes_vals)
        status = "✅" if is_valid else "❌"
        print(f"   {status} Diabetes_012: {diabetes_vals}")
        if not is_valid:
            all_valid = False
    
    # 6. Verificar otras columnas
    print(f"\n5. Verificación de otras columnas:")
    other_checks = {
        'GenHlth': [1, 2, 3, 4, 5],
        'Age': list(range(1, 14)),
        'Education': list(range(1, 7)),
        'Income': list(range(1, 9))
    }
    
    for col, expected in other_checks.items():
        if col in df_clean.columns:
            unique_vals = sorted(df_clean[col].unique())
            is_valid = all(val in expected for val in unique_vals)
            status = "✅" if is_valid else "❌"
            print(f"   {status} {col}: {unique_vals}")
            if not is_valid:
                all_valid = False
    
    print(f"\n6. RESULTADO FINAL:")
    if all_valid:
        print("   ✅ ÉXITO: Todos los datos están en los rangos correctos")
        print("   ✅ El problema de valores 7, 9, etc. ha sido solucionado")
    else:
        print("   ❌ PROBLEMA: Todavía hay valores fuera de rango")
    
    print(f"\n7. Distribución de Diabetes:")
    if 'Diabetes_012' in df_clean.columns:
        diabetes_counts = df_clean['Diabetes_012'].value_counts().sort_index()
        total = len(df_clean)
        for val, count in diabetes_counts.items():
            class_name = {0: "No diabetes", 1: "Pre-diabetes", 2: "Diabetes"}[val]
            pct = (count/total)*100
            print(f"   {val} ({class_name}): {count:,} ({pct:.1f}%)")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

print("\n=== PRUEBA COMPLETADA ===")
