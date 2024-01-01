#!/usr/bin/env python

"""
Summary:
The Python program will be used to understand 
the differences between MAG and OpenAlex. 

We want to understand the number of records 
missing in OA that are present in MAG.
We will do so by checking the following:

1. # of magpids missing in OA (by year)
2. # of dois in MAG missing in OA (by year)
3. publication types of magpids, and dois of those missing in OA
"""

import pandas as pd
import os
from config_reader import read_config

def read_csv(file_path, columns):
    # reading only relevant columns
    return pd.read_csv(file_path, usecols=columns).drop_duplicates()

def fix_doi_column(df):
    # fixing doi to remove leading url and retain just the DOI (to merge)
    df.loc[:, 'doi'] = df['doi'].str.split("https://doi.org/").str[-1]

def main():
    paths = read_config()
    # Reading OA work ids
    OA_WORKS_IDS_PATH = paths['OA_WORKS_IDS_PATH']
    # Reading MAG papers.txt
    MAG_PAPERS_PATH = paths['MAG_PAPERS_PATH']


    # reading open alex ids
    df_oa = read_csv(OA_WORKS_IDS_PATH, None)  # Read all columns for now
    # filter openalex column (was added later after creating file from this code)
    df_oa = df_oa.drop(columns=['openalex','pmid'])
    # data processing to fix columns
    fix_doi_column(df_oa)
    
    # reading the MAG papers.txt
    df_papers = pd.read_csv(MAG_PAPERS_PATH, sep="\t", header=None, 
                            usecols=[0,2,3,7])\
                                .rename(columns={0:'MAGPID',
                                                2:'MAGDOI',
                                                3:'MAGDocType',
                                                7:'MAGPubYear'})
    
    """
    Check 1: 
    Checking the number of MAGPIDs missing in 
    OpenAlex dataset
    """
    
    # number of magpids in mag
    num_unique_magpids = df_papers['MAGPID'].nunique()
    print(f"Number of mag pids in MAG {num_unique_magpids}")
    
    
    df_papers_not_in_oa = df_papers[~df_papers['MAGPID'].isin(df_oa['mag'])]\
                            .drop_duplicates()
                            
    num_unique_magpids_not_in_oa = df_papers_not_in_oa['MAGPID'].nunique()
    print(f"Number of mag pids missing in OA {num_unique_magpids_not_in_oa}")
    
    # Now let us check the doc type of those and from which years are they
    print("Printing distribution of those missing by DocType Top 30")
    print(df_papers_not_in_oa['MAGDocType'].value_counts().head(30))
    
    print("Printing distribution of those missing by year Top 30")
    print(df_papers_not_in_oa['MAGPubYear'].value_counts().head(30))
    
    del df_papers_not_in_oa
    
    """
    Check 2: 
    Checking the number of DOIs in MAG missing in 
    OpenAlex dataset
    """
    
    # number of DOIs in mag
    num_unique_dois = df_papers['MAGDOI'].nunique()
    print(f"Number of DOIs in MAG {num_unique_dois}")
    
    df_papers_not_in_oa = df_papers[~df_papers['MAGDOI'].isin(df_oa['doi'])]\
                            .drop_duplicates()
                            
    num_unique_dois_not_in_oa = df_papers_not_in_oa['MAGDOI'].nunique()
    print(f"Number of mag pids missing in OA {num_unique_dois_not_in_oa}")
    
    # Now let us check the doc type of those and from which years are they
    print("Printing distribution of those missing by DocType Top 30")
    print(df_papers_not_in_oa['MAGDocType'].value_counts().head(30))
    
    print("Printing distribution of those missing by year Top 30")
    print(df_papers_not_in_oa['MAGPubYear'].value_counts().head(30))

if __name__ == "__main__":
    main()