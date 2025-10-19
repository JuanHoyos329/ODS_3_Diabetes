import pandas as pd
import logging
import os

def load_child_mortality_data(file_path: str = "data/raw/child_mortality_indicators_col.csv") -> pd.DataFrame:
    
    try:
        # Verificar que el archivo existe
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        # Leer CSV, saltando la primera fila que contiene metadatos
        df = pd.read_csv(file_path, skiprows=[1])
        
        logging.info(f"[OK] Datos de mortalidad infantil cargados exitosamente")
        logging.info(f"  - Archivo: {file_path}")
        logging.info(f"  - Registros: {len(df):,}")
        logging.info(f"  - Columnas: {len(df.columns)}")
        
        # Mostrar información básica del dataset
        if 'Period' in df.columns:
            years = df['Period'].dropna().unique()
            logging.info(f"  - Años disponibles: {len(years)} ({min(years)} - {max(years)})")
        
        return df
        
    except FileNotFoundError as e:
        logging.error(f"[ERROR] Archivo no encontrado: {e}")
        raise
    except Exception as e:
        logging.error(f"[ERROR] Error al cargar datos de mortalidad: {e}")
        raise


def extract_socioeconomic_data_from_api(
    country_code: str = "COL",
    start_year: int = 1950,
    end_year: int = 2022,
    indicators: list = None
) -> pd.DataFrame:
    
    import requests
    import time
    
    try:
        # Indicadores socioeconómicos predefinidos del Banco Mundial
        if indicators is None:
            indicators = {
                'SP.POP.TOTL': 'population_total',           # Población total
                'NY.GDP.PCAP.CD': 'gdp_per_capita',          # PIB per cápita
                'SI.POV.NAHC': 'poverty_headcount_ratio',    # Tasa de pobreza
                'SE.PRM.ENRR': 'school_enrollment_primary',  # Matriculación primaria
                'SP.DYN.CDRT.IN': 'mortality_rate',          # Tasa de mortalidad infantil
                'SP.URB.TOTL.IN.ZS': 'urban_population_percent'  # % población urbana
            }
        
        logging.info(f"Consultando API del Banco Mundial...")
        logging.info(f"  - País: {country_code}")
        logging.info(f"  - Periodo: {start_year}-{end_year}")
        logging.info(f"  - Indicadores: {len(indicators)}")
        
        all_data = []
        
        # Consultar cada indicador
        for indicator_code, indicator_name in indicators.items():
            url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}"
            params = {
                'date': f'{start_year}:{end_year}',
                'format': 'json',
                'per_page': 500
            }
            
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # La API devuelve [metadata, datos]
                if len(data) > 1 and data[1]:
                    for record in data[1]:
                        all_data.append({
                            'year': int(record['date']),
                            'indicator': indicator_name,
                            'value': record['value']
                        })
                    
                    logging.info(f"  [OK] {indicator_name}: {len(data[1])} registros")
                else:
                    logging.warning(f"  [!] {indicator_name}: Sin datos")
                
                # Pequeña pausa para no saturar la API
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                logging.warning(f"  [X] Error consultando {indicator_name}: {e}")
                continue
        
        # Convertir a DataFrame
        if not all_data:
            logging.warning("No se obtuvieron datos de la API")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        
        # Pivotar para tener un indicador por columna
        df_pivot = df.pivot(index='year', columns='indicator', values='value').reset_index()
        
        logging.info(f"[OK] Datos socioeconomicos extraidos exitosamente")
        logging.info(f"  - Años únicos: {len(df_pivot)}")
        logging.info(f"  - Indicadores: {len(df_pivot.columns) - 1}")
        
        return df_pivot
        
    except Exception as e:
        logging.error(f"[ERROR] Error extrayendo datos de API: {e}")
        raise


def extract_colombian_socioeconomic_data() -> pd.DataFrame:
    
    logging.info("Función plantilla: extract_colombian_socioeconomic_data()")
    logging.info("Se recomienda usar extract_socioeconomic_data_from_api() para datos del Banco Mundial")
    
    # Retornar DataFrame vacío como placeholder
    return pd.DataFrame()