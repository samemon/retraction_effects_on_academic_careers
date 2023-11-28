#!/usr/bin/env python

"""
Summary:
This Python program will extract work_ids 
for retraction notes using the following logic:

1. First it will use RW, to extract DOIs and PubMedIDs 
of retraction notes if the DOI and PubMedID is not 
equal to the original paper's DOI. 

2. Second, it will load Bedoor's identified retraction 
notes from MAG using MAGIDs.

3. Then, it will load the openAlex work ids file 
to identify work ids based on three matches (doi, pmid, mag).

4. We shall save these work ids as identified retraction notes, 
and remove them whenever computing a confounder. 
"""

import pandas as pd
import os
from config_reader import read_config
from data_utils import read_csv, fix_pmid_column, fix_doi_column

def main():
    # reading all the relevant paths
    paths = read_config()
    OUTDIR = paths['OUTDIR_PATH']
    # Add path to retraction watch original dataset
    RW_CSV_PATH = paths['RW_CSV_PATH']
    # Add path to author_ids of retracted authors
    PROCESSED_RETRACTION_NOTICES_MAG_PATH = paths['PROCESSED_RETRACTION_NOTICES_MAG_PATH']
    # Add path to your OpenAlex works_ids.csv.gz file
    OA_WORKS_IDS_PATH = paths['OA_WORKS_IDS_PATH']
    
    # reading retraction watch
    df_rw = pd.read_csv(RW_CSV_PATH)
    # extracting dois for notes
    dois_notes = df_rw[~df_rw['RetractionDOI'].isin(['unavailable', 'Unavailable']) & 
                            ~df_rw['RetractionDOI'].isna() &
                            ~df_rw['OriginalPaperDOI'].eq(df_rw['RetractionDOI'])]\
                            ['RetractionDOI'].unique()
    
    # extracting pmids for notes
    pmids_notes = df_rw[~df_rw['RetractionPubMedID'].eq(0) & 
                        ~df_rw['RetractionPubMedID'].isna() &
                        ~df_rw['OriginalPaperPubMedID'].eq(df_rw['RetractionPubMedID'])]\
                            ['RetractionPubMedID'].unique()
    
    # reading retraction notes from MAG
    magpid_notes = pd.read_csv(PROCESSED_RETRACTION_NOTICES_MAG_PATH)\
                        ['PID'].unique()
                        
    # reading works file from OA
    df_oa = read_csv(OA_WORKS_IDS_PATH, None)  # Read all columns for now
    # filter openalex column
    df_oa = df_oa.drop(columns=['openalex'])
    
    # Data processing to fix columns
    
    fix_pmid_column(df_oa)
    fix_doi_column(df_oa)
    
    # Now filtering to extract relevant work ids
    df_oa = df_oa[df_oa['mag'].isin(magpid_notes) | df_oa['doi'].isin(dois_notes) | df_oa['pmid'].isin(pmids_notes)]
    
    # saving
    df_oa.to_csv(os.path.join(OUTDIR, "retraction_notes_ids_OA.csv"),
                    index=False)
    
if __name__ == "__main__":
    main()