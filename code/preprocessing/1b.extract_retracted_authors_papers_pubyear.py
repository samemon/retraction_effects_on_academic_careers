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
    - MAGTitle
    - MAGAID
    - MAGAffID
    - MAGAuthorOrder
    - MAGPubYear
    - MAGAuthorName
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
                                .rename(columns={'MAGPID':'RetractedPaperMAGPID'})\
                                .drop_duplicates() # there should be none though
    
    # reading the MAG paper_author_affiliation file
    # we will read four columns: paper ids, author ids, affiliations, and author seq
    df_papers_authors_aff = pd.read_csv(MAG_PAPER_AUTHOR_AFF_PATH, sep="\t", header=None, 
                            usecols=[0,1,2,3])\
                                .rename(columns={0:'MAGPID',
                                                1:'MAGAID',
                                                2:'MAGAffID',
                                                3:'MAGAuthorOrder'})\
                                .drop_duplicates()
    
    # Let us first extract retracted authors
    df_retracted_authors = df_papers_authors_aff[df_papers_authors_aff['MAGPID']\
                                            .isin(df_mag_rw_papers['RetractedPaperMAGPID'])]
    
    
    # Let us merge it with the retraction watch columns
    df_retracted_authors = df_retracted_authors.merge(df_mag_rw_papers, 
                                                    left_on='MAGPID', right_on='RetractedPaperMAGPID')
    
    # Only keeping useful columns
    df_retracted_authors = df_retracted_authors[['MAGAID','Record ID', 
                                                'RetractedPaperMAGPID', 'RetractionYear']]
        
    # Now let us extract all their papers
    # This df contains: MAGPID, MAGAID, MAGAffID, MAGAuthorOrder
    # We shall merge it later with others
    df_retracted_authors_papers = df_papers_authors_aff[df_papers_authors_aff['MAGAID']\
                                            .isin(df_retracted_authors['MAGAID'])]
    
    del df_papers_authors_aff # make some space
    
    # Now that we have all the papers, let us extract pub year
    # reading the MAG papers.txt
    df_papers = pd.read_csv(MAG_PAPERS_PATH, sep="\t", header=None, 
                            usecols=[0,4,7])\
                                .rename(columns={0:'MAGPID',
                                                4:'MAGTitle',
                                                7:'MAGPubYear'})\
                                .drop_duplicates()
    
    # This df contains MAGPID, MAGTitle, MAGPubYear
    # We shall merge this later
    df_paper_pubyear = df_papers[df_papers['MAGPID'].isin(df_retracted_authors_papers['MAGPID'])]
    
    del df_papers
    # Now we extract the author names
    # reading the MAG authors.txt
    df_authors = pd.read_csv(MAG_AUTHORS_PATH, sep="\t", header=None, 
                            usecols=[0,2])\
                                .rename(columns={0:'MAGAID',
                                                2:'MAGAuthorName'})\
                                .drop_duplicates()
    # This dataframe contains MAGAID, MAGAuthorName
    # We shall merge this later                            
    df_author_names = df_authors[df_authors['MAGAID'].isin(df_retracted_authors['MAGAID'])]
    
    del df_authors
    
    # MERGING STEP
    df_merged1 = df_retracted_authors_papers\
                    .merge(df_paper_pubyear, on='MAGPID', how='left')
                    
    df_merged2 = df_merged1.merge(df_author_names, on='MAGAID', how='left')\
    
    df_merged3 = df_merged2.merge(df_retracted_authors, on='MAGAID')\
                    .drop_duplicates()
                        
    # Save the relevant data
    df_merged3.to_csv(os.path.join(OUTDIR_PATH, "Retracted_Authors_PubHistories.csv"), 
                        index=False)
    
if __name__ == "__main__":
    main()