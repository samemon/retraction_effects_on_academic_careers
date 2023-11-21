#!/usr/bin/env python

"""
Summary:
This Python program focuses on extracting all 
the works by each retracted author along with each 
paper's publication year, title, 
venue/source (id + name + type), and type of article. 
This will help us compute downstream variables such 
as academic age, # of papers prior to retraction, 
number of collaborators, and so on.
"""

import pandas as pd
import configparser
import os
import gc

def read_config():
    # preprocessing the config file
    config = configparser.ConfigParser()
    config.read('preprocessing_config.ini')
    return config['Paths']

def main():
    # reading all the relevant paths
    paths = read_config()
    # path to output directory
    OUTDIR = paths['OUTDIR_PATH']
    
    # path to works authorships file from open alex
    OA_WORKS_AUTHORSHIPS_PATH = paths['OA_WORKS_AUTHORSHIPS_PATH']
    # path to works file from open alex
    OA_WORKS_PATH = paths['OA_WORKS_PATH']
    # path to work ids and their source (venue) ids.
    OA_WORKS_PRIMARY_LOCATIONS_PATH = paths['OA_WORKS_PRIMARY_LOCATIONS_PATH']
    # path to source/venues and their info
    OA_SOURCES_PATH = paths['OA_SOURCES_PATH']
    # path to work ids from open alex for retracted authors
    PROCESSED_RETRACTED_AUTHOR_IDS_PATH = paths['PROCESSED_RETRACTED_AUTHOR_IDS_PATH']
    
    # read the oa ids of retracted authors
    set_rw_oa_author_ids = pd.read_csv(PROCESSED_RETRACTED_AUTHOR_IDS_PATH,
                                    usecols=['author_id'])['author_id'].unique()
    
    # read all the work-authorship pairs
    df_oa_works_authorships = pd.read_csv(OA_WORKS_AUTHORSHIPS_PATH, 
                                        usecols=['work_id', 'author_id'])\
                                        .drop_duplicates()
                                        
    # filter df_oa_works_authorships to only include relevant authors
    df_oa_works_authorships = df_oa_works_authorships[df_oa_works_authorships['author_id'].isin(set_rw_oa_author_ids)]
    
    # read all the work ids from open alex (this will take most of the memory)
    df_oa_works = pd.read_csv(OA_WORKS_PATH, 
                    usecols=['id','title','publication_year'])\
                    .rename(columns={'id':'work_id'})\
                    .drop_duplicates()
                    
    # extracting works for retracted authors
    lst_works_for_retracted_authors = df_oa_works_authorships['work_id'].unique()
    
    # filtering works to only include works of retracted authors
    df_oa_works = df_oa_works[df_oa_works['work_id'].isin(lst_works_for_retracted_authors)]
    
    # merging works and authors
    # This dataframe will have  author_id, work_id, title, publication_year
    df_oa_authors_works_retracted = df_oa_works_authorships.merge(df_oa_works,
                                                    on='work_id')\
                                                    .drop_duplicates()
    
    # let us clean up some memory
    del df_oa_works
    del df_oa_works_authorships
    del lst_works_for_retracted_authors
    gc.collect()
    
    # now we will extract information about sources
    df_oa_works_sources = pd.read_csv(OA_WORKS_PRIMARY_LOCATIONS_PATH,
                                    usecols=['work_id', 'source_id'])\
                                    .drop_duplicates()
    
    # reading sources
    df_oa_sources = pd.read_csv(OA_SOURCES_PATH, 
                                usecols=['id', 'display_name', 'type'])
    

if __name__ == "__main__":
    main()