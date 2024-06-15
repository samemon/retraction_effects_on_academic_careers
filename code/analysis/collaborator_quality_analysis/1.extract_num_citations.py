#!/usr/bin/env python

"""
Summary:
This Python program focuses on extracting the number of 
citations for all the collaborators for all the years. 
We will have a csv with four columns:
- MAGAID
- Year
- numCitations
- CumCitations
"""

import pandas as pd
import os

# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

INDIR_MAG = "/scratch/bka3/MAG_2021/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where match files are
INDIR_MATCHING = INDIR_PROCESSED + "/author_matching/"

def extract_author_histories():
    # Let us first read the authors that are relevant
    # In this case, we are interested in retracted authors + matches
    # matches up till the point of intersection.py
    
    df_matched = pd.read_csv(INDIR_MATCHING+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020.csv",
                            usecols=['MAGCollabAID'])
    
    df_matched['MAGCollabAID'] = df_matched['MAGCollabAID'].astype(int)
    
    # Add path to your MAG papers file
    MAG_PAPER_AUTHOR_AFF_PATH = INDIR_MAG+"/PaperAuthorAffiliations.txt"
    
    df_papers_authors_aff = pd.read_csv(MAG_PAPER_AUTHOR_AFF_PATH, sep="\t", header=None, 
                            usecols=[0,1])\
                                .rename(columns={0:'MAGPID',
                                                1:'MAGAID'})\
                                .drop_duplicates()
    
    df_papers_authors_aff = df_papers_authors_aff[df_papers_authors_aff['MAGAID'].isin(df_matched['MAGCollabAID'])]
    
    return df_papers_authors_aff
    

def main():
    
    #outdir = "/scratch/sm9654/data/MAG_2021/derived/"
    
    # Reading all the relevant authors and their publication histories
    # df_authorhistories = pd.read_csv("/scratch/sm9654/retraction_openalex/data/processed/Retracted_Authors_PubHistories.csv",
    #                                 usecols=['MAGPID', 'MAGAID'])\
    #                                 .drop_duplicates()
    
    # Reading all the relevant authors and their publication histories 
    # For round2, we are only doing authors that are retracted and their matches                         
    df_authorhistories = extract_author_histories()
    
    # Reading the retraction notices
    notices = pd.read_csv("/scratch/sm9654/retraction_openalex/data/processed/retraction_notices_postfiltering.csv")\
                    .rename(columns={'PID':'MAGPID'})['MAGPID'].unique()

    # Reading the references file
    # RID is the ID of the referenced paper
    # PID is the ID of the paper referencing RID
    df_rid_pidyear = pd.read_csv("/scratch/bka3/PaperReferencesPubYears.csv",
                                usecols=['PID', 'RID','PID_PubYear'])\
                                    .replace({"1981-01-31": 1981.0,
                                            "1999-01-31": 1999.0,
                                            "2015-01-31": 2015.0,
                                            "2015-12-14": 2015.0,
                                            "2016-09-29": 2016.0,
                                            "2014-01-31": 2014.0})\
                                    .drop_duplicates()
    df_rid_pidyear['PID_PubYear'] = df_rid_pidyear['PID_PubYear'].astype(float)
    
    # We will remove all the notices
    df_rid_pidyear = df_rid_pidyear[~df_rid_pidyear['PID'].isin(notices) & 
                                    ~df_rid_pidyear['RID'].isin(notices)]
    
    
    
    # Removing notices from histories
    df_authorhistories = df_authorhistories[~df_authorhistories['MAGPID']\
                                .isin(notices)]
    
    # Now we merge authors papers (MAGPID) with those referenced (RID)
    # And once we have merged, we will count the unique PIDs for each year
    df_merged = df_authorhistories.merge(df_rid_pidyear, 
                                    left_on='MAGPID',
                                    right_on='RID')
    
    # Now computing number of citations for each paper for each year
    # For each author and year, we will count the number of unique
    # references (PIDs). 
    # NOTE: This was nunique but in conv with Bedoor, it is count now
    # To ensure multiple citations in the same year by same paper are counted 
    df_numCitations = df_merged\
            .groupby(['MAGAID','PID_PubYear'])['PID']\
            .count().reset_index()\
            .rename(columns={'PID':'numCitations'})

    # Computing cumulative citations for each author up to each year
    
    df_numCitations['cumCitations'] = df_numCitations\
            .sort_values(by="PID_PubYear")\
            .groupby(["MAGAID"])['numCitations'].transform('cumsum')
            
    # df_numCitations = df_numCitations.\
    #             drop(columns=['MAGPID'])
    
    df_numCitations.to_csv(INDIR_DERIVED+"/AID_year_newCitations_cumCitations_corrected_collaborators.csv",
            index=False)

if __name__ == "__main__":
    main()