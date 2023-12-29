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

def find_paper_matches(retracted_papers, path_oa_works, year, nchoices=3):
    """This function will find string matches for retracted paper 
    titles from open alex works using fuzzy matching. For now, we will 
    find 3 matches per paper. For each paper we will only look at 
    papers from the year +-1 of the retracted paper's publication year.
    
    Args:
        retracted_papers ([list]): list of titles of retracted papers
        path_oa_works ([string]): path to Open Alex works object
        year ([int/float]): year of the retracted papers
        nchoices ([int]): number of matches per paper
    """
    
    
    df_works = pd.read_csv(path_oa_works, 
                        usecols=['id','title','publication_year'])
    
    # extracting relevant works
    df_works['YearDiff'] = df_works['publication_year'] - year
    df_works = df_works[df_works['YearDiff'].isin([-1,0,1])]\
                    .drop(columns=['YearDiff'])\
                    .rename(columns={'id':'work_id'})
    # reseting index so we can track the id's later
    df_works = df_works.reset_index(drop=True)
    # normalizing the title
    df_works['NormTitle'] = df_works['title'].str.lower().str.strip()
    
    # normalizing titles of retracted papers
    retracted_papers = [title.lower().strip() for title in retracted_papers]
    
    # We will save matches in this list
    lst_matches = []
    
    # Finding the top matches for each paper 
    for title in retracted_papers:
        matches = process.extract(title, df_works['NormTitle'], limit=nchoices)
        # processing matches
        for match,score,index in matches:
            matched_id_title_pubyear = df_works.loc[index].tolist()
            lst_matches.append([title, match, score, index]+matched_id_title_pubyear)
    
    columns=['RWTitle','OAMatchTitle','score', 'index', 'work_id',
                'OATitle','publication_year','OANormTitle']
    
    return pd.DataFrame(lst_matches, columns=columns)

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
    df_rw_relevant = df_rw[['Record ID', 'RWTitleNorm']].drop_duplicates()
    
    
    # reading the MAG papers.txt
    df_papers = pd.read_csv(MAG_PAPERS_PATH, sep="\t", header=None, 
                            usecols=[0,2,4,7])\
                                .rename(columns={0:'MAGPID',
                                                2:'OriginalPaperDOI',
                                                4:'MAGTitle',
                                                7:'MAGPubYear'})
    
    df_papers['MAGPubYear'] = pd.to_numeric(df_papers['MAGPubYear'], errors='coerce')
    
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