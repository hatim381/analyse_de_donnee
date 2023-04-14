import pandas as pd

def convert_csv_to_parquet(csv_file_path, parquet_file_path):
    df = pd.read_csv(csv_file_path, sep=";")
    df.to_parquet(parquet_file_path)



