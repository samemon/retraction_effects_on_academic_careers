#!/usr/bin/env python

"""
Summary:
The Python program will be used to map the 
retraction watch papers to the relevant 
papers in OA using fuzzy matching.
"""


import pandas as pd
import os
from config_reader import read_config
from data_utils import read_csv, fix_pmid_column, fix_doi_column
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
    
    
    df_works = read_csv(path_oa_works, columns=['id','title','publication_year'])
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
    OUTDIR = paths['OUTDIR_PATH']
    # Add path to your RW Original dataset
    RW_CSV_PATH = paths['RW_CSV_PATH']
    # Add path to your OpenAlex works_ids.csv.gz file
    OA_WORKS_IDS_PATH = paths['OA_WORKS_IDS_PATH']
    
    # Read datasets
    df_rw = read_csv(RW_CSV_PATH, ['Title'])
    df_mag_rw = read_csv(MAG_RW_CSV_PATH, ['Record ID', 'MAGPID'])
    df_oa = read_csv(OA_WORKS_IDS_PATH, None)  # Read all columns for now
    