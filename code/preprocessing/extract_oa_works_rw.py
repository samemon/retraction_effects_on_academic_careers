#!/usr/bin/env python

import pandas as pd
import configparser
import os

def read_config():
    config = configparser.ConfigParser()
    config.read('preprocessing_config.ini')
    return config['Paths']

def read_csv(file_path, columns):
    # reading only relevant columns
    return pd.read_csv(file_path, usecols=columns).drop_duplicates()

def fix_pmid_column(df):
    # fixing pmid to remove leading url and retain just the id (to merge)
    df.loc[:, 'pmid'] = df['pmid'].str.split("/").str[-1]

def main():
    # reading all the relevant paths
    paths = read_config()
    OUTDIR = paths['OUTDIR']
    RW_CSV_PATH = paths['RW_CSV_PATH']
    MAG_RW_CSV_PATH = paths['MAG_RW_CSV_PATH']
    OA_CSV_PATH = paths['OA_CSV_PATH']

    # Read datasets
    df_rw = read_csv(RW_CSV_PATH, ['Record ID', 'OriginalPaperDOI', 'OriginalPaperPubMedID'])
    df_mag_rw = read_csv(MAG_RW_CSV_PATH, ['Record ID', 'MAGPID'])
    df_oa = read_csv(OA_CSV_PATH, None)  # Read all columns for now

    # Merge datasets to retain all 3 possible ids
    df_mag_rw = df_rw.merge(df_mag_rw, on='Record ID', how='left')

    # Data processing
    fix_pmid_column(df_oa)

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
