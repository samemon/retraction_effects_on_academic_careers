#!/usr/bin/env python

"""
Summary:
The Python program will be used to filter 
the irrelevant records from retraction watch
"""

import pandas as pd
import os
from config_reader import read_config
from data_utils import read_csv

def main():
    # reading all the relevant paths
    paths = read_config()
    INDIR_RW_PATH = paths['INDIR_RW_PATH']
    # Add path to your RW Original dataset
    RW_CSV_PATH = paths['RW_CSV_PATH']
    
    # Read datasets
    # Reading retraction watch with the relevant columns only
    df_rw = pd.read_csv(RW_CSV_PATH).drop_duplicates()
    
    # preprocessing original paper date for later (part b of paper matching)
    # extracting publication year
    # some of these will have no publication date as a result of this
    df_rw['OriginalPaperDate'] = pd.to_datetime(df_rw['OriginalPaperDate'], 
                                                format='%d/%m/%Y %H:%M', 
                                                errors='coerce')
    
    df_rw['OriginalPaperYear'] = df_rw['OriginalPaperDate'].dt.year
    
    # processing title
    df_rw['RWTitleNorm'] = df_rw['Title'].str.lower() 
    
    # preprocessing retraction date to remove all that were retracted post 2015
    df_rw['RetractionDate'] = pd.to_datetime(df_rw['RetractionDate'], 
                                                format='%d/%m/%Y %H:%M', 
                                                errors='coerce')
    
    df_rw['RetractionYear'] = df_rw['RetractionDate'].dt.year
    
    # Let us identify all papers retracted between 1990 and 2015 (inclusive)
    # Let us also print # of papers that fall out of this range. 
    
    num_papers_1990_2015 = df_rw[df_rw['RetractionYear'].ge(1990) & df_rw['RetractionYear'].le(2015)]\
                                ['Record ID'].nunique()
    
    num_papers_not_1990_2015 = df_rw[df_rw['RetractionYear'].lt(1990) | df_rw['RetractionYear'].gt(2015)]\
                                ['Record ID'].nunique()
    
    print(f"Number of retracted papers between 1990 and 2015 {num_papers_1990_2015}")
    print(f"Number of retracted papers beyond 1990 and 2015 {num_papers_not_1990_2015}")

    # filtering (Note only one paper has NaN retraction year from 1700s)
    df_rw = df_rw[df_rw['RetractionYear'].ge(1990) & df_rw['RetractionYear'].le(2015)]
    
    # Save the relevant data
    
    # We shall save this version of retraction watch to use it for later processing.
    df_rw.to_csv(os.path.join(INDIR_RW_PATH, "RW_1990_2015.csv"), index=False)
    
if __name__ == "__main__":
    main()