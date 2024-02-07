#!/usr/bin/env python

"""
Summary:
The Python program will be used to 
extract retracted authors and their 
publication histories. We will do so 
as follows:

1. First we will extract all retracted 
authors based on the RW-MAG record matching 
we did in earlier stages.
2. Second we will extract all the papers 
by those authors, their affiliations, 
and their author order in each of their papers. 
3. Next we will extract the publication year 
for each paper.
4. Next we will extract first and last name 
of each author.
5. Finally we shall save a dataframe with the 
following columns:
    - MAGPID
    - MAGAID
    - MAGAffID
    - MAGAuthorOrder
    - MAGPubYear
    - MAGFirstName
    - MAGLastName
    - Record ID
    - RetractionYear
    - RetractedPaperMAGPID
"""

import pandas as pd
import os
from config_reader import read_config
from data_utils import read_csv

def main():
    # reading all the relevant paths
    paths = read_config()
    # Reading the final MAG-RW paper matched file
    PROCESSED_RW_MAG_FINAL_PAPER_MATCHES = paths['PROCESSED_RW_MAG_FINAL_PAPER_MATCHES']
    # Add path to your MAG papers file
    MAG_PAPER_AUTHOR_AFF_PATH = paths['MAG_PAPER_AUTHOR_AFF_PATH']
    # Add path to your MAG papers file
    MAG_PAPERS_PATH = paths['MAG_PAPERS_PATH']
    # Add path to MAG authors file
    MAG_AUTHORS_PATH = paths['MAG_AUTHORS_PATH']
    # Add path to output directory
    OUTDIR_PATH = paths['OUTDIR_PATH']
    
    # Read datasets
    # Reading retraction watch with the relevant columns only
    df_mag_rw_papers = pd.read_csv(PROCESSED_RW_MAG_FINAL_PAPER_MATCHES,
                                usecols=['Record ID', 'MAGPID', 'RetractionYear'])\
                                .drop_duplicates() # there should be none though
    
    # reading the MAG paper_author_affiliation file
    # we will read four columns: paper ids, author ids, affiliations, and author seq
    df_paper_authors_aff = pd.read_csv(MAG_PAPER_AUTHOR_AFF_PATH, sep="\t", header=None, 
                            usecols=[0,1,2,3])\
                                .rename(columns={0:'MAGPID',
                                                1:'MAGAID',
                                                2:'MAGAffID',
                                                3:'MAGAuthorOrder'})\
                                .drop_duplicates()
    
    # Let us first extract retracted authors
    
    
    
    # Save the relevant data
    df.to_csv(os.path.join(OUTDIR_PATH, "Retracted_Authors_PubHistories.csv"), 
                        index=False)
    
    
if __name__ == "__main__":
    main()