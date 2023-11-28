#utils.py
import pandas as pd

def read_csv(file_path, columns):
    # reading only relevant columns
    return pd.read_csv(file_path, usecols=columns).drop_duplicates()

def fix_pmid_column(df):
    # fixing pmid to remove leading url and retain just the id (to merge)
    df.loc[:, 'pmid'] = df['pmid'].str.split("/").str[-1]

def fix_doi_column(df):
    # fixing doi to remove leading url and retain just the DOI (to merge)
    df.loc[:, 'doi'] = df['doi'].str.split("https://doi.org/").str[-1]
