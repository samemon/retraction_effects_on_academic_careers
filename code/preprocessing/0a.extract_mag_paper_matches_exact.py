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
2) pmid
3) exact title match

In later parts we will match based on 
4) fuzzy title match in the same year
5) fuzzy title match in the year +- 1
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

def main(year):
    # reading all the relevant paths
    paths = read_config()
    OUTDIR_FUZZYMATCH_PATH = paths['OUTDIR_FUZZYMATCH_PATH']
    # Add path to your RW Original dataset
    RW_CSV_PATH = paths['RW_CSV_PATH']
    # Add path to your OpenAlex works.csv.gz file
    OA_WORKS_PATH = paths['OA_WORKS_PATH']
    
    # Read datasets
    df_rw = read_csv(RW_CSV_PATH, ['Title', 'OriginalPaperDate'])\
                    .drop_duplicates()
    
    # extracting publication year
    # some of these will have no publication date as a result of this
    df_rw['OriginalPaperDate'] = pd.to_datetime(df_rw['OriginalPaperDate'], 
                                                format='%d/%m/%Y %H:%M', 
                                                errors='coerce')
    
    df_rw['OriginalPaperYear'] = df_rw['OriginalPaperDate'].dt.year

    # filtering
    df_rw = df_rw[df_rw.OriginalPaperYear.eq(year) | df_rw.OriginalPaperYear.isna()]
    
    retracted_papers = df_rw['Title'].unique().tolist()
    
    num_retracted_papers = len(retracted_papers)
    print(f"Number of retracted papers processing in {year}: {num_retracted_papers}")
    
    df_matched = find_paper_matches(retracted_papers, OA_WORKS_PATH, year)
    
    # Save the relevant data
    filename = f"works_ids_RW_OA_fuzzymatched_{year}.csv"
    df_matched.to_csv(os.path.join(OUTDIR_FUZZYMATCH_PATH, filename), index=False)
    
    
if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Fuzzy-matching paper titles")
    
    # Add command line argument for the year
    parser.add_argument("--year", type=float, help="Year of publication for retracted papers", required=True)

    # Parse the command line arguments
    args = parser.parse_args()

    # Call the main function with the parsed year argument
    main(args.year)