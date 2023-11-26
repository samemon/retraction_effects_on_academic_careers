#!/usr/bin/env python

"""
Summary:
The Python program is designed to merge the RW-MAG records
with OA works using the following logic:
1. First merging is done based on DOI of the retracted paper/work (RW-OA)
2. Second merging is done based on PubMedID of the retracted paper/work (RW-OA)
3. Third merging is done based on MAG ID of the retracted paper (RW-MAG-OA)
"""

import pandas as pd
import os
from config_reader import read_config

def read_csv(file_path, columns):
    # reading only relevant columns
    return pd.read_csv(file_path, usecols=columns).drop_duplicates()

def main():
    # reading all the relevant paths
    paths = read_config()
    OUTDIR = paths['OUTDIR_PATH']
    # Add path to your RW Original dataset
    RW_CSV_PATH = paths['RW_CSV_PATH']
    # Add path to MAG-merged RW dataset containing MAGPID for each Record
    MAG_RW_CSV_PATH = paths['MAG_RW_CSV_PATH']
    # Path to processed retracted works
    PROCESSED_RETRACTED_WORK_IDS_PATH = paths['PROCESSED_RETRACTED_WORK_IDS_PATH']
    

    # Read datasets
    df_rw = read_csv(RW_CSV_PATH, ['Record ID', 'OriginalPaperDOI', 'OriginalPaperPubMedID'])
    df_mag_rw = read_csv(MAG_RW_CSV_PATH, ['Record ID', 'MAGPID'])
    df_oa_rw_works = read_csv(PROCESSED_RETRACTED_WORK_IDS_PATH, ['work_id', 'doi', 'mag', 'pmid'])
    
    # Let us now merge df_rw and df_mag_rw to create one dataframe of IDs
    df_mag_rw2 = df_rw.merge(df_mag_rw, on='Record ID', how='left')\
                        .rename(columns={'OriginalPaperDOI': 'doi',
                                    'OriginalPaperPubMedID': 'pmid',
                                    'MAGPID': 'mag'})
    
    # merging based on DOI
    df_mag_rw_DOI = df_mag_rw2.merge(df_oa_rw_works, on='doi')
    
    # merging based on pubmed id
    df_mag_rw_PMID = df_mag_rw2.merge(df_oa_rw_works, on='pmid')
    # filtering those already merged based on DOI
    RIDs_merged_onDOI = df_mag_rw_DOI['Record ID'].unique()
    df_mag_rw_PMID = df_mag_rw_PMID[~df_mag_rw_PMID['Record ID']\
                                    .isin(RIDs_merged_onDOI)]
    
    # merging based on mag id
    df_mag_rw_MAGPID = df_mag_rw2.merge(df_oa_rw_works, on='mag')
    # filtering those already merged based on DOI and PMID
    RIDs_merged_onPMID = df_mag_rw_PMID['Record ID'].unique()
    df_mag_rw_MAGPID = df_mag_rw_MAGPID[~df_mag_rw_MAGPID['Record ID']\
                                    .isin(RIDs_merged_onDOI) & ~df_mag_rw_MAGPID['Record ID']\
                                    .isin(RIDs_merged_onPMID)]
    
    # concatenating all the three merges
    df_merged = pd.concat([df_mag_rw_DOI, df_mag_rw_PMID, df_mag_rw_MAGPID])
    
    # saving 
    df_merged.to_csv(os.path.join(OUTDIR, "works_ids_RW_MAG_OA_merged_sample_BasedOndoi_THENpmid_THENmag.csv"), index=False)
    
    # printing number of works
    print("Number of merged RW-MAG-OA ids based on doi/pmid/mag:", df_merged['work_id'].nunique(),
        "Number of merged RW-MAG-OA Record IDs:", df_merged['Record ID'].nunique())

if __name__ == "__main__":
    main()
    
    
    