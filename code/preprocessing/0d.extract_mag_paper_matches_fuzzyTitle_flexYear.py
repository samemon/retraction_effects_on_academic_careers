#!/usr/bin/env python

"""
Summary:
The Python program will be used to map the 
retraction watch papers to the relevant 
papers in MAG using fuzzy matching. 

This is the third of four parts 
of fuzzy matching where we will match 
based on 
3) fuzzy title match in the same year

In later parts we will match based on 

4) fuzzy title match in the year +- 1
"""

import pandas as pd
import os
import argparse
from config_reader import read_config
from data_utils import read_csv
from rapidfuzz import process, fuzz
import numpy as np

def find_paper_matches(df_rw_relevant, path_mag_papers, year, nchoices=3):
    """This function will find string matches for retracted paper 
    titles from MAG papers using fuzzy matching. For now, we will 
    find 3 matches per paper. For each paper we will only look at 
    papers from the exact year of the retracted paper's publication year.
    
    Args:
        retracted_papers ([list]): list of titles of retracted papers
        path_mag_papers ([string]): path to MAG papers.txt
        year ([int/float]): year of the retracted papers
        nchoices ([int]): number of matches per paper
    """
    
    # reading the MAG papers.txt
    df_papers = pd.read_csv(path_mag_papers, sep="\t", header=None, 
                            usecols=[0,4,7])\
                                .rename(columns={0:'MAGPID',
                                                4:'MAGTitle',
                                                7:'MAGPubYear'})
                                
    # converting mag pub year to numeric
    df_papers['MAGPubYear'] = pd.to_numeric(df_papers['MAGPubYear'], errors='coerce')
    
    # Limiting the papers to relevant given year
    df_papers = df_papers[df_papers['MAGPubYear'].eq(year-1) | df_papers['MAGPubYear'].eq(year+1)]
    
    # reseting index so we can track the id's later
    df_papers = df_papers.reset_index(drop=True)
    
    # normalizing titles of retracted papers
    retracted_papers = df_rw_relevant['RWTitleNorm'].unique().tolist()
    
    # We will save matches in this list
    lst_matches = []
    
    # Finding the top matches for each paper 
    for title in retracted_papers:
        matches = process.extract(title, df_papers['MAGTitle'], limit=nchoices)
        # processing matches
        for match,score,index in matches:
            matched_id_title_pubyear = df_papers.loc[index].tolist()
            lst_matches.append([title, match, score, index]+matched_id_title_pubyear)
    
    # We are saving MAGTitle twice just to check the logic is working
    
    columns=['RWTitleNorm','MAGTitle', 'score', 'index', 'MAGPID',
                'MAGTitle','MAGPubYear']
    
    df_matched = pd.DataFrame(lst_matches, columns=columns)
    
    # Removing all matches with score < 90
    df_matched = df_matched[df_matched['score'] > 90]
    
    # merging with retraction watch to extract record id
    df_matched = df_matched.merge(df_rw_relevant, on='RWTitleNorm')
    
    return df_matched

def main(year, n_splits, n):
    # reading all the relevant paths
    paths = read_config()
    OUTDIR_FUZZYMATCH_PATH = paths['OUTDIR_FUZZYMATCH_PATH']
    # Add path to your RW Original preprocessed dataset
    RW_ORIGINAL_W_YEAR_PATH = paths['RW_ORIGINAL_W_YEAR_PATH']
    # Path to records that were already matched in previous step
    PROCESSED_RW_MAG_EXACT_PAPER_MATCHES = paths['PROCESSED_RW_MAG_EXACT_PAPER_MATCHES']
    # Add path to your MAG papers file
    MAG_PAPERS_PATH = paths['MAG_PAPERS_PATH']
    
    # Read datasets
    # Reading retraction watch with the relevant columns only
    df_rw = pd.read_csv(RW_ORIGINAL_W_YEAR_PATH)
    
    # Reading records that are already matched
    lst_rw_exact_matched = pd.read_csv(PROCESSED_RW_MAG_EXACT_PAPER_MATCHES,
                                    usecols=['Record ID'])['Record ID'].unique()
    
    # Removing records that are already matched
    df_rw = df_rw[~df_rw['Record ID'].isin(lst_rw_exact_matched)]
    # Removing records that were published in the year other than given year
    df_rw = df_rw[df_rw['OriginalPaperYear'] == year]
    # Sorting by record ids
    df_rw = df_rw.sort_values(by='Record ID')
    # Splitting into n 
    split_dfs = np.array_split(df_rw, n_splits)
    # Extracting the nth split
    df_rw = split_dfs[n]
    
    num_records_RW = df_rw['Record ID'].nunique()
    print(f"Number of records in RW published in {year} in split {n}: {num_records_RW}")
    
    # only extracting relevant columns
    df_rw_relevant = df_rw[['Record ID', 'RWTitleNorm']].drop_duplicates()
    
    df_matched = find_paper_matches(df_rw_relevant, MAG_PAPERS_PATH, year)
    
    # Identifying number of records matched with > 90 score
    num_papers_matched = df_matched['Record ID'].nunique()
    print(f"Number of records matched based on fuzzy title in exact year {year} in split {n} : {num_papers_matched}")
    
    # Save the relevant data
    filename = f"RW_MAG_fuzzy_paper_flex_year_matched_{year}"
    if(n_splits > 1):
        filename = f"{filename}_{n}.csv"
    else:
        filename = f"{filename}.csv"
    df_matched.to_csv(os.path.join(OUTDIR_FUZZYMATCH_PATH, filename), index=False)
    
    

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Fuzzy-matching paper titles for +-1 year")
    
    # Add command line argument for the year
    parser.add_argument("--year", type=float, help="Year of publication for retracted papers", required=True)
    parser.add_argument("--splits", type=str, help="number of splits", required=False, default=1)
    parser.add_argument("--n", type=str, help="which split is the current one", required=False, default=0)
    # Parse the command line arguments
    args = parser.parse_args()

    # print year for tracking
    print(args.year, args.splits, args.n)
    
    # Call the main function with the parsed year argument
    main(args.year, args.splits, args.n)