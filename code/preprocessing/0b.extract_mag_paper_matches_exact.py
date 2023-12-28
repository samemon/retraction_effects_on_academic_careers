#!/usr/bin/env python

"""
Summary:
The Python program will be used to map the 
retraction watch papers to the relevant 
papers in MAG using fuzzy matching. 

This is the second of four parts 
of fuzzy matching where we will match 
based on 
1) doi
2) exact title match

In later parts we will match based on 
3) fuzzy title match in the same year
4) fuzzy title match in the year +- 1
"""

import pandas as pd
import os
from config_reader import read_config
from data_utils import read_csv

def main():
    # reading all the relevant paths
    paths = read_config()
    OUTDIR_FUZZYMATCH_PATH = paths['OUTDIR_FUZZYMATCH_PATH']
    # Add path to your RW Original dataset
    RW_1990_2015_PATH = paths['RW_1990_2015_PATH']
    # Add path to your MAG papers file
    MAG_PAPERS_PATH = paths['MAG_PAPERS_PATH']
    
    # Read datasets
    # Reading retraction watch with the relevant columns only
    df_rw = pd.read_csv(RW_1990_2015_PATH)
    
    # only extracting relevant columns
    df_rw_relevant = df_rw[['Record ID', 'RWTitleNorm', 'OriginalPaperDOI']].drop_duplicates()
    
    
    # reading the MAG papers.txt
    df_papers = pd.read_csv(MAG_PAPERS_PATH, sep="\t", header=None, 
                            usecols=[0,2,4,7])\
                                .rename(columns={0:'MAGPID',
                                                2:'OriginalPaperDOI',
                                                4:'MAGTitle',
                                                7:'MAGPubYear'})
    
    # Limiting the papers to relevant window
    df_papers = df_papers[df_papers['MAGPubYear'].ge(1989) & 
                        df_papers['MAGPubYear'].le(2016)]
    
    # only extracting non-nan dois
    df_rw_relevant_doi = df_rw_relevant[~df_rw_relevant['OriginalPaperDOI'].isna()]
    
    # merging on doi
    df_merged_doi = df_papers[df_papers['OriginalPaperDOI']\
            .isin(df_rw_relevant_doi['OriginalPaperDOI'].unique())]
    
    df_merged_doi = df_rw_relevant.merge(df_merged_doi, 
                                            on='OriginalPaperDOI', 
                                            how='inner')
    
    num_merged_by_doi = df_merged_doi['Record ID'].nunique()
    print(f"Number of records matched based on doi: {num_merged_by_doi}")
    
    # merging on title
    df_merged_title = df_papers[df_papers['MAGTitle']\
            .isin(df_rw_relevant['RWTitleNorm'].unique())]
    
    df_merged_title = df_rw_relevant.merge(df_merged_title, 
                                            left_on='RWTitleNorm', 
                                            right_on='MAGTitle', 
                                            how='inner')
    
    # removing records merged on doi
    df_merged_title = df_merged_title[~df_merged_title['Record ID']\
                        .isin(df_merged_doi['Record ID'].unique())]
    
    num_merged_by_title = df_merged_title['Record ID'].nunique()
    print(f"Number of records matched based on exact title: {num_merged_by_title}")
    
    # concatenating
    df_matched = pd.concat([df_merged_doi, df_merged_title])
    
    num_papers_matched = df_matched['Record ID'].nunique()
    
    print(f"Number of records matched based on doi and exact title {num_papers_matched}")
    
    # Save the relevant data
    df_matched.to_csv(os.path.join(OUTDIR_FUZZYMATCH_PATH, "RW_MAG_exact_paper_matched.csv"), 
                        index=False)
    
    
if __name__ == "__main__":
    main()