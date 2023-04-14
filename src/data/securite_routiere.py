import pandas as pd

# Lecture du fichier CSV en utilisant la fonction read_csv() de pandas
df = pd.read_csv(r"C:\Users\hatim\Documents\Analyse_de_donnee_avec_python\securite_routiere\data\raw\carcteristiques-2021.csv", sep=";")

# Enregistrement du dataframe en format parquet dans le dossier data/interim
df.to_parquet(r"C:\Users\hatim\Documents\Analyse_de_donnee_avec_python\securite_routiere\data\interim/carcteristiques-2021.parquet")
