from weather.data.load import load_all_stations


def run_ingestion():
    """
    Load raw CSV data and save as Parquet (staging layer).
    """

    df = load_all_stations("data/raw")

    df.write_parquet("data/staging/all_stations.parquet")

    print("Datos guardados en archivo parquet")


if __name__ == "__main__":
    run_ingestion()
