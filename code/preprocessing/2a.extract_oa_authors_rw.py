#!/usr/bin/env python

"""
Summary:
This Python program will extract author information 
for authors of retracted papers listed in OpenAlex. 
"""

import pandas as pd
import os
from config_reader import read_config

def main():
    # reading all the relevant paths
    paths = read_config()
    OUTDIR = paths['OUTDIR_PATH']
    # Add path to OpenAlex works_authorships.csv.gz file
    OA_WORKS_AUTHORSHIPS_PATH = paths['OA_WORKS_AUTHORSHIPS_PATH']
    # Add path to your OpenAlex merged RW dataset with OpenAlex work ids
    # This can be generated from 1.extract_oa_works_rw.py
    OA_RW_WORK_IDS_PATH = paths['PROCESSED_RETRACTED_WORK_IDS_PATH']
    
    # reading the RW MAG merged file from openAlex
    df_oa_rw = pd.read_csv(OA_RW_WORK_IDS_PATH)

    # Reading open alex works authorships file
    df_oa_authors = pd.read_csv(OA_WORKS_AUTHORSHIPS_PATH)

    # Extracting relevant work ids
    oa_work_ids = df_oa_rw['work_id'].unique()

    # Only extracting authors from relevant rw works
    df_oa_rw_authors = df_oa_authors[df_oa_authors['work_id'].isin(oa_work_ids)]

    # Save the relevant data
    df_oa_rw_authors.to_csv(os.path.join(OUTDIR, "authors_ids_RW_MAG_OA.csv"), index=False)

    # Print results
    print("Number of RW-MAG-OA authors:", df_oa_rw_authors['author_id'].nunique())

if __name__ == "__main__":
    main()