import pandas as pd
import numpy as np

def rename_colonne(df):
    return df.rename(columns={"lum": "luminosity", "int": "intersection", "agg": "agglomeration", "atm": "weather", "col": "collision"})

 
#Good practices

def df_format(df):
    #Changer les types object en category
    obj_cols = list(df.select_dtypes("object").columns)
    for c in obj_cols:
        df[c] = df[c].astype("category")
    #Changer les types int64 en int64, int32, int16 ou int8 suivant le max
    int_cols = list(df.select_dtypes("integer").columns)
    encodings = [np.int8, np.int16, np.int32]
    for c in int_cols:
        m = np.abs(df[c]).max()
        for e in encodings:
            if m < np.iinfo(e).max:
                df[c] = e(df[c])
                break

    #Changer les types float64 en float32
    float_cols = list(df.select_dtypes("float").columns)
    for c in float_cols:
        df[c] = np.float32(df[c])

def convert_csv_to_parquet(csv_file_path, parquet_file_path):
    df = pd.read_csv(csv_file_path/"carcteristiques-2021.csv", sep=";", decimal=',', parse_dates=[['an', 'mois', 'jour', 'hrmn']])
    df = rename_colonne(df)
    df_format(df)
    df.to_parquet(parquet_file_path/"carcteristiques-2021.parquet")


"""
def print_memory(df, cols=None):
    #Prints the memory consumed by each individual column of the DataFrame.
    cols = cols or df.columns
    mem = np.round(df[cols].memory_usage(deep=True) / 1e6, 1)
    print("total_memory = {}MB".format(np.round(mem.sum(), 1)))
    return mem
"""
#Pour run : python -m src.data.make_dataset data\raw data\interim
#python -m src.data.securite_routiere data\raw data\interim