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
    
    
    
if __name__ == "__main__":
    main()