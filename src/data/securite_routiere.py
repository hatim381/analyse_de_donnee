import pandas as pd
import numpy as np

def convert_csv_to_parquet(csv_file_path, parquet_file_path):
    df = pd.read_csv(csv_file_path/"carcteristiques-2021.csv", sep=";",decimal=',',parse_dates= [['an','mois','jour','hrmn']],infer_datetime_format=True)
    df = df.rename(columns={"lum":"luminosity", "int":"intersection", "agg":"agglomeration", "atm":"weather", "col":"collision"})
    df.to_parquet(parquet_file_path/"carcteristiques-2021.parquet")


def print_memory(df, cols=None):
    """
    Prints the memory consumed by each individual column of the DataFrame.
    """
    cols = cols or df.columns
    mem = np.round(df[cols].memory_usage(deep=True) / 1e6, 1)
    print("total_memory = {}MB".format(np.round(mem.sum(), 1)))
    return mem

#Pour run : python -m src.data.make_dataset data\raw data\interim