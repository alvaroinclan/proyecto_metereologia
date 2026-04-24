import os
from weather.data.load import load_grib_data_in_batches

def run_ingestion():
    """
    Load raw GRIB data, extract 50 locations in batches, and save as Parquet (staging layer).
    """
    input_path = "data/raw/data.grib"
    output_path = "data/staging/all_stations.parquet"
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print("Iniciando ingestión de datos desde GRIB...")
    df = load_grib_data_in_batches(input_path, batch_size=10)
    
    df.write_parquet(output_path)
    print(f"Datos guardados en archivo parquet: {output_path}")

if __name__ == "__main__":
    run_ingestion()
