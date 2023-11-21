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
import os
from config_reader import read_config
import gc

def main():
    # reading all the relevant paths
    paths = read_config()
    # path to output directory
    OUTDIR = paths['OUTDIR_PATH']
    
    # path to works authorships file from open alex
    OA_WORKS_AUTHORSHIPS_PATH = paths['OA_WORKS_AUTHORSHIPS_PATH']
    # path to works file from open alex
    OA_WORKS_PATH = paths['OA_WORKS_PATH']
    # path to works with ids (doi, pmid, mag)
    OA_WORKS_IDS_PATH = paths['OA_WORKS_IDS_PATH']
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
    set_works_for_retracted_authors = df_oa_works_authorships['work_id'].unique()
    
    # filtering works to only include works of retracted authors
    df_oa_works = df_oa_works[df_oa_works['work_id'].isin(set_works_for_retracted_authors)]
    
    # merging works and authors
    # This dataframe will have  author_id, work_id, title, publication_year
    df_oa_retracted_authors_works = df_oa_works_authorships.merge(df_oa_works,
                                                    on='work_id')\
                                                    .drop_duplicates()
    
    # let us clean up some memory
    del df_oa_works
    del df_oa_works_authorships
    gc.collect()
    
    # now we will extract source id information about openalex works for RW authors
    df_oa_works_sources = pd.read_csv(OA_WORKS_PRIMARY_LOCATIONS_PATH,
                                    usecols=['work_id', 'source_id'])\
                                    .drop_duplicates()
    
    # filtering work-sources to remove works that are irrelevant
    df_oa_works_sources = df_oa_works_sources[df_oa_works_sources['work_id'].isin(set_works_for_retracted_authors)]
    
    # reading openalex sources
    df_oa_sources = pd.read_csv(OA_SOURCES_PATH, 
                                usecols=['id', 'display_name', 'type'])\
                                .rename(columns={'id':'source_id',
                                                'display_name': 'source_name',
                                                'type': 'source_type'})\
                                .drop_duplicates()
    
    
    # merging to augment more info about sources
    # this dataframe has following cols: work_id, source_id, source_name, source_type
    df_oa_works_sources = df_oa_works_sources.merge(df_oa_sources, on='source_id',
                                                    how='left')
    
    # let us merge this info with the retracted authors works
    df_oa_retracted_authors_works_sources = df_oa_retracted_authors_works.merge(df_oa_works_sources,
                                                                                on='work_id',
                                                                                how='left')
    
    # now we need to add more identifiers to works
    df_oa_works_ids = pd.read_csv(OA_WORKS_IDS_PATH)\
                        .drop(columns=['openalex'])\
                        .drop_duplicates()
    
    # only extracting relevant works
    df_oa_works_ids = df_oa_works_ids[df_oa_works_ids['work_id'].isin(set_works_for_retracted_authors)]

    # finally merging these ids with df_oa_retracted_authors_works_sources
    df_oa_retracted_authors_works_sources_ids = df_oa_retracted_authors_works_sources.merge(df_oa_works_ids,
                                                                                            on='work_id', how='left')
    
    # saving the file
    df_oa_retracted_authors_works_sources_ids.to_csv(os.path.join(OUTDIR, "all_OA_works_authorship_sources_forRetractedAuthors.csv"),
                                                                index=False)

    # printing for sanity checks
    print("Number of retracted authors:", df_oa_retracted_authors_works_sources_ids['author_id'].nunique(),
        "Total number of works", df_oa_retracted_authors_works_sources_ids['work_id'].nunique(),
        "Columns: ", df_oa_retracted_authors_works_sources_ids.columns)

if __name__ == "__main__":
    main()