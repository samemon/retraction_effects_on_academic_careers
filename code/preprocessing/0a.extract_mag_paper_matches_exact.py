#!/usr/bin/env python

"""
Summary:
The Python program will be used to map the 
retraction watch papers to the relevant 
papers in MAG using fuzzy matching. 

This is the first of three parts 
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
    RW_CSV_PATH = paths['RW_CSV_PATH']
    # Add path to your MAG papers file
    MAG_PAPERS_PATH = paths['MAG_PAPERS_PATH']
    
    # Read datasets
    # Reading retraction watch with the relevant columns only
    df_rw = pd.read_csv(RW_CSV_PATH).drop_duplicates()
    
    # preprocessing original paper date for later (part b of paper matching)
    # extracting publication year
    # some of these will have no publication date as a result of this
    df_rw['OriginalPaperDate'] = pd.to_datetime(df_rw['OriginalPaperDate'], 
                                                format='%d/%m/%Y %H:%M', 
                                                errors='coerce')
    
    df_rw['OriginalPaperYear'] = df_rw['OriginalPaperDate'].dt.year
    
    # processing title
    df_rw['RWTitleNorm'] = df_rw['Title'].str.lower() 
    
    # preprocessing retraction date to remove all that were retracted post 2015
    df_rw['RetractionDate'] = pd.to_datetime(df_rw['RetractionDate'], 
                                                format='%d/%m/%Y %H:%M', 
                                                errors='coerce')
    
    df_rw['RetractionYear'] = df_rw['RetractionDate'].dt.year
    
    # Let us identify all papers retracted between 1990 and 2015 (inclusive)
    # Let us also print # of papers that fall out of this range. 
    
    num_papers_1990_2015 = df_rw[df_rw['RetractionYear'].ge(1990) & df_rw['RetractionYear'].le(2015)]\
                                ['Record ID'].nunique()
    
    num_papers_not_1990_2015 = df_rw[df_rw['RetractionYear'].lt(1990) | df_rw['RetractionYear'].gt(2015)]\
                                ['Record ID'].nunique()
    
    print(f"Number of retracted papers between 1990 and 2015 {num_papers_1990_2015}")
    print(f"Number of retracted papers beyond 1990 and 2015 {num_papers_not_1990_2015}")

    # filtering (Note only one paper has NaN retraction year from 1700s)
    df_rw = df_rw[df_rw['RetractionYear'].ge(1990) & df_rw['RetractionYear'].le(2015)]
    
    # reading the MAG papers.txt
    df_papers = pd.read_csv(MAG_PAPERS_PATH, sep="\t", header=None, 
                            usecols=[0,2,4])\
                                .rename(columns={0:'MAGPID',
                                                2:'OriginalPaperDOI',
                                                4:'MAGTitle'})
    
    # only extracting relevant columns
    df_rw_relevant = df_rw[['Record ID', 'RWTitleNorm', 'OriginalPaperDOI']].drop_duplicates()
    
    # merging on doi
    df_merged_doi = df_rw_relevant.merge(df_papers, on='OriginalPaperDOI', 
                                            how='inner')
    
    # merging on title
    df_merged_title = df_rw_relevant.merge(df_papers, left_on='RWTitleNorm', right_on='MAGTitle', 
                                            how='inner')
    
    # concatenating
    df_matched = pd.concat([df_merged_doi, df_merged_title])
    
    # Save the relevant data
    
    # We shall save this version of retraction watch to use it for later processing.
    df_rw.to_csv(os.path.join(OUTDIR_FUZZYMATCH_PATH, "RW_1990_2015.csv", index=False))
    
    df_matched.to_csv(os.path.join(OUTDIR_FUZZYMATCH_PATH, "RW_MAG_exact_paper_matched.csv"), 
                        index=False)
    
    
if __name__ == "__main__":
    main()