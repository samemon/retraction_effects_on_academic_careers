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

def find_paper_matches(retracted_papers, path_oa_works, year, n_choices=3):
    """This function will find string matches for retracted paper 
    titles from open alex works using fuzzy matching. For now, we will 
    find 3 matches per paper. For each paper we will only look at 
    papers from the year +-1 of the retracted paper's publication year.
    
    Args:
        retracted_papers ([list]): list of titles of retracted papers
        path_oa_works ([string]): path to Open Alex works object
        year ([int/float]): year of the retracted papers
    """

    

def main():
    # reading all the relevant paths
    paths = read_config()
    OUTDIR = paths['OUTDIR_PATH']
    # Add path to your RW Original dataset
    RW_CSV_PATH = paths['RW_CSV_PATH']
    # Add path to MAG-merged RW dataset containing MAGPID for each Record
    MAG_RW_CSV_PATH = paths['MAG_RW_CSV_PATH']
    # Add path to your OpenAlex works_ids.csv.gz file
    OA_WORKS_IDS_PATH = paths['OA_WORKS_IDS_PATH']