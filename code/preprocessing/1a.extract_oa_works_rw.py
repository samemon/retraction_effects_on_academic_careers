#!/usr/bin/env python

"""
Summary:
The Python program is designed to extract work_id information 
for papers listed on Retraction Watch by merging data from 
Retraction Watch and OpenAlex database. 
The program utilizes three key identifiers-
DOI (Digital Object Identifier), 
PubMedID, and 
MAG ID (Microsoft Academic Graph ID)—
to match and consolidate information from both sources.
"""


import pandas as pd
import os
from config_reader import read_config
from data_utils import read_csv, fix_pmid_column, fix_doi_column

def main():
    # reading all the relevant paths
    paths = read_config()
    OUTDIR = paths['OUTDIR_PATH']
    # Add path to your RW Original dataset
    RW_CSV_PATH = paths['RW_CSV_PATH']
    # Add path to MAG-merged RW dataset containing MAGPID for each Record
    MAG_RW_CSV_PATH = paths['MAG_RW_CSV_PATH']
    # Add path to your OpenAlex works_ids.csv.gz file
    OA_WORKS_IDS_PATH = paths['OA_WORKS_IDS_PATH']

    # Read datasets
    df_rw = read_csv(RW_CSV_PATH, ['Record ID', 'OriginalPaperDOI', 'OriginalPaperPubMedID'])
    df_mag_rw = read_csv(MAG_RW_CSV_PATH, ['Record ID', 'MAGPID'])
    df_oa = read_csv(OA_WORKS_IDS_PATH, None)  # Read all columns for now
    
    # filter openalex column (was added later after creating file from this code)
    df_oa = df_oa.drop(columns=['openalex'])

    # Merge datasets to retain all 3 possible ids
    df_mag_rw = df_rw.merge(df_mag_rw, on='Record ID', how='left')

    # Data processing to fix columns
    
    fix_pmid_column(df_oa)
    fix_doi_column(df_oa)

    # extracting all kinds of relevant IDs
    dois_rw = df_mag_rw[~df_mag_rw['OriginalPaperDOI'].isin(['unavailable', 'Unavailable']) & ~df_mag_rw['OriginalPaperDOI'].isna()]['OriginalPaperDOI'].unique()
    pmids_rw = df_mag_rw[~df_mag_rw['OriginalPaperPubMedID'].eq(0) & ~df_mag_rw['OriginalPaperPubMedID'].isna()]['OriginalPaperPubMedID'].unique()
    magids_rw = df_mag_rw[~df_mag_rw['MAGPID'].isna()]['MAGPID'].unique()

    # only extracting those rows that are relevant in terms of retracted papers
    df_oa = df_oa.loc[df_oa['doi'].isin(dois_rw) | df_oa['pmid'].isin(pmids_rw) | df_oa['mag'].isin(magids_rw), :]

    # Save the relevant data
    df_oa.to_csv(os.path.join(OUTDIR, "works_ids_RW_MAG_OA_merged_sample_BasedOndoi_ORpmid_ORmag.csv"), index=False)

    # Print results
    print("Number of merged RW-MAG-OA ids based on doi/pmid/mag:", df_oa['work_id'].nunique())

if __name__ == "__main__":
    main()
