#!/usr/bin/env python

import pandas as pd
import configparser
import os

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')

OUTDIR = config['Paths']['OUTDIR']
RW_CSV_PATH = config['Paths']['RW_CSV_PATH']
MAG_RW_CSV_PATH = config['Paths']['MAG_RW_CSV_PATH']
OA_CSV_PATH = config['Paths']['OA_CSV_PATH']

# Let us first read the original RW dataset
df_rw = pd.read_csv(RW_CSV_PATH, usecols=['Record ID','OriginalPaperDOI',
                                        'OriginalPaperPubMedID'])\
        .drop_duplicates()
                        
# Reading the mag merged RW dataset
df_mag_rw = pd.read_csv(MAG_RW_CSV_PATH, usecols=['Record ID', 'MAGPID'])\
                .drop_duplicates()

# merging the two but retaining the original (merge left)
df_mag_rw = df_rw.merge(df_mag_rw, on='Record ID', how='left')

# Reading open alex (dropping openalex as it is duplicate column)                        
df_oa = pd.read_csv(OA_CSV_PATH, compression="gzip")\
        .drop(columns=['openalex'])
# Fixing OA's pmid column for easy merging
df_oa['pmid'] = df_oa['pmid'].str.split("/").str[-1]

# Extracting dois from RW (those that are available)
dois_rw = df_mag_rw[~df_mag_rw['OriginalPaperDOI'].isin(['unavailable','Unavailable']) &
                ~df_mag_rw['OriginalPaperDOI'].isna()]\
                ['OriginalPaperDOI'].unique()
                
# Extracing pub-med-ids (those that are available)   
pmids_rw = df_mag_rw[~df_mag_rw['OriginalPaperPubMedID'].eq(0) & 
                ~df_mag_rw['OriginalPaperPubMedID'].isna()]\
                ['OriginalPaperPubMedID'].unique()

# Extracing mag-ids (those that were merged)
magids_rw = df_mag_rw[~df_mag_rw['MAGPID'].isna()]['MAGPID'].unique()

# filtering the openalex dataset
df_oa = df_oa[df_oa['doi'].isin(dois_rw) | df_oa['pmid'].isin(pmids_rw) | df_oa['mag'].isin(magids_rw)]

# saving the relevant OA
df_oa.to_csv(os.path.join(OUTDIR, "works_ids_RW_MAG_OA_merged_sample_BasedOndoi_ORpmid_ORmag.csv"), 
                index=False)

# Printing number of work ids after filtering based on RW
print("Number of merged RW-MAG-OA ids based on doi/pmid/mag:", df_oa['work_id'].nunique())
