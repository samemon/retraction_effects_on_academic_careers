#!/usr/bin/env python

"""
Summary:
This Python program will extract retraction year 
for each author. Repeated offenders will have 
multiple retractions and hence multiple retraction 
year. 

The output for this program will be a file with 
the following columns:
author_id, Record ID, retraction year
"""

import pandas as pd
import os
from config_reader import read_config

def main():
    # reading all the relevant paths
    paths = read_config()
    OUTDIR = paths['OUTDIR_PATH']
    # Add path to retraction watch original dataset
    RW_CSV_PATH = paths['RW_CSV_PATH']
    # Add path to author_ids of retracted authors
    PROCESSED_RETRACTED_AUTHOR_IDS_PATH = paths['PROCESSED_RETRACTED_AUTHOR_IDS_PATH']
    # Add path to work_ids of retracted papers
    PROCESSED_RETRACTED_MERGED_WORK_IDS_PATH = paths['PROCESSED_RETRACTED_MERGED_WORK_IDS_PATH']
    
    # reading retraction watch
    df_rw = pd.read_csv(RW_CSV_PATH, usecols=['Record ID', 'RetractionDate'])\
                    .drop_duplicates()
    
    # extracting retraction year
    df_rw['RetractionDate'] = pd.to_datetime(df_rw['RetractionDate'], 
                                                format='%d/%m/%Y %H:%M', 
                                                errors='coerce')
    
    df_rw['RetractionYear'] = df_rw['RetractionDate'].dt.year
    
    # removing retraction date
    df_rw = df_rw.drop(columns=['RetractionDate'])
    
    # reading retracted papers work ids 
    df_oa_works_retracted = pd.read_csv(PROCESSED_RETRACTED_MERGED_WORK_IDS_PATH,
                                        usecols=['Record ID','work_id'])\
                                            .drop_duplicates()

    # merging only those that were merged with OA
    df_rw_work_id = df_rw.merge(df_oa_works_retracted, 
                                on='Record ID', how='right')
    
    # reading authors
    df_rw_authors = pd.read_csv(PROCESSED_RETRACTED_AUTHOR_IDS_PATH,
                                usecols=['work_id', 'author_id'])\
                                .drop_duplicates()
                                
    # merging authors with retracted paper work_ids
    df_rw_authors_works = df_rw_work_id.merge(df_rw_authors, 
                                                on='work_id',
                                                how='left')
    
    print("Number of authors:", df_rw_authors_works['author_id'].nunique(),
        "Number of works:", df_rw_authors_works['work_id'].nunique())
    
    # Save the relevant data
    df_rw_authors_works.to_csv(os.path.join(OUTDIR, "authors_records_retractionYear.csv"), index=False)

if __name__ == "__main__":
    main()