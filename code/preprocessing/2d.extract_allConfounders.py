#!/usr/bin/env python

"""
Summary:
This Python program focuses on extracting the 
confounders for papers and authors. 

For papers, these confounders will be:

1. Venue (i.e. journal or conference), 
We will extract MAGJCName, MAGJournalType, MAGJCID
2. Year of Retraction (from RW)
3. # of authors (from RW) and from MAG
4. Reason (from processed file)
5. Journal or conference ranking (from Scimago rankings)

For authors, these confounders will be:
1. Gender (from MAG derivatives file)
2. Discipline (from MAG derivatives file)
3. Author order (from MAG processed file on pub histories)
4. Affiliation rank (from MAG using retraction year)
5. Academic age or first pub year (from MAG derivatives)
6. Number of papers (from MAG derivatives)
7. Number of citations (from MAG derivatives)
8. Number of collaborators (from MAG derivatives)
"""

import pandas as pd
import os
from config_reader import read_config

def extract_venue(magpids, indir):
    
    # Reading PIDs with Journal or Conference IDs
    df_papers = pd.read_csv(indir+"/Papers.txt", 
                    header=None, sep="\t",
                    usecols=[0,8,11,12])\
                        .rename(columns={0:"RetractedPaperMAGPID",
                                        8:"dateobject", 
                                        11:"JID",
                                        12:"CSID"})
    
    df_papers = df_papers[df_papers['RetractedPaperMAGPID'].isin(magpids)]
    
    # Reading journals tables
    df_journals = pd.read_csv(indir+"/Journals.txt", header=None, sep="\t",
                                usecols=[0,2])\
                                .rename(columns={0:"JID",
                                2: "JournalName"})
    
    # Reading conference series table
    df_conference_series = pd.read_csv(indir+"/ConferenceSeries.txt",
                                        header=None, sep="\t",
                                usecols=[0,3])\
                                .rename(columns={0:"CSID",
                                3: "ConferenceSeriesName"})

    df_papers['JID'] = df_papers['JID'].astype(float)
    df_papers['CSID'] = df_papers['CSID'].astype(float)
    df_conference_series['CSID'] = df_conference_series['CSID'].astype(float)
    df_journals['JID'] = df_journals['JID'].astype(float)
    
    # Merging Journal and Conferences information to papers
    df_papers = df_papers.merge(df_journals, on="JID", how="left")
    df_papers = df_papers.merge(df_conference_series, on="CSID", how="left")
    
    return df_papers

def extract_reason(df_authors, indir):
    df_reasons = pd.read_csv(indir+"propagated_reasons_for_paper_matched_sample.csv",
                            usecols=['ReasonPropagatedMajorityOfMajority', 'Record ID'])\
                                .drop_duplicates()
    df_authors = df_authors[['Record ID', 'RetractedPaperMAGPID']]\
                    .drop_duplicates()
    df_merged = df_authors.merge(df_reasons, on='Record ID', how='left')\
                        .drop(columns='Record ID')
    
    return df_merged

def extract_num_authors_rw(df_authors, indir):
    # Reading rw
    df_rw = pd.read_csv(indir+"RW_Original_wYear.csv")
    df_rw = df_rw[~df_rw['Author'].isna()]
    
    # Function to count non-empty authors in a column
    def count_non_empty_authors(column):
        authors = column.split(";")
        return sum(1 for author in authors if author.strip())

    # Apply the function to the "Author" column and create a new column "Author_Count"
    df_rw['NumAuthorsInRetractedPaperRW'] = df_rw['Author'].apply(count_non_empty_authors)
    
    df_authors = df_authors[['Record ID', 'RetractedPaperMAGPID']]\
                            .drop_duplicates()
    
    df_merged = df_authors.merge(df_rw, on='Record ID', how='left')\
                    [['RetractedPaperMAGPID', 'NumAuthorsInRetractedPaperRW']]\
                        .drop_duplicates()
                        
    return df_merged

def extract_gender(magaids, indir):
    df_gender = pd.read_csv(indir + "AID_firstname_gender.csv",
                            usecols=['AID','gender','confidence'])
    df_gender = df_gender[df_gender['AID'].isin(magaids)]
    df_gender = df_gender.rename(columns={'AID':'MAGAID',
                                        'gender':'GenderizeGender',
                                        'confidence':'GenderizeConfidence'})
    return df_gender

def extract_author_order(magaids, indir):
    df_histories = pd.read_csv(indir+"Retracted_Authors_PubHistories.csv")
    df_histories = df_histories[df_histories['MAGPID'] == df_histories['RetractedPaperMAGPID']]
    df_histories = df_histories[['MAGAID','MAGAuthorOrder']].drop_duplicates()
    df_histories = df_histories[df_histories['MAGAID'].isin(magaids)]
    return df_histories

def extract_age(df_authors, indir):
    df_age = pd.read_csv(indir+"AID_PID_firstPubYear.csv")\
                        .rename(columns={'AID':'MAGAID',
                                        'PID':'FirstPubMAGPID',
                                        'year':'FirstPubYear'})
    
    df_authors = df_authors[['MAGAID','RetractionYear']].drop_duplicates()
    
    df_age = df_authors.merge(df_age, on='MAGAID', how='left')
    
    df_age['AcademicAge'] = df_age['RetractionYear'] - df_age['FirstPubYear']
    
    df_age = df_age.drop(columns=['RetractionYear'])

    return df_age

def extract_disciplines(indir):
    dfs = []
    for f in os.listdir(indir):
        dfi = pd.read_csv(indir+f)
        dfs.append(dfi)
    df = pd.concat(dfs)[['MAGAID','MAGrootFID', 'MAGrootFIDMaxPercent']]\
                    .drop_duplicates()
    return df

def extract_numPapers(df_authors, indir):
    df_authors = df_authors[['MAGAID', 'RetractionYear']].drop_duplicates()
    
    df_numpapers = pd.read_csv(indir+"MAGAID_PubYear_numPapers_cumPapers_correctedPostNHB.csv",
                            usecols=['MAGAID', 'year', 'cumPapers'])\
                                .rename(columns={'year':'cumPapersYear'})
    
    df_numpapers = df_numpapers[df_numpapers['MAGAID'].isin(df_authors['MAGAID'])]
    
    df_merged = df_authors.merge(df_numpapers, on='MAGAID', how='left')
    
    df_merged = df_merged[df_merged['cumPapersYear']\
                            .le(df_merged['RetractionYear'])]
    
    df_merged = df_merged.sort_values(by='cumPapersYear', ascending=False)
    
    df_merged = df_merged.drop_duplicates(subset=['MAGAID'], keep='first')
    
    df_merged = df_merged.drop(columns=['RetractionYear'])
    
    return df_merged
    

def extract_numCitations(df_authors, indir):
    
    df_authors = df_authors[['MAGAID', 'RetractionYear']].drop_duplicates()
    
    df_numcitations = pd.read_csv(indir+"AID_year_newCitations_cumCitations_corrected.csv",
                            usecols=['MAGAID', 'PID_PubYear', 'cumCitations'])\
                                .rename(columns={'PID_PubYear':'cumCitationsYear'})
    
    df_numcitations = df_numcitations[df_numcitations['MAGAID'].isin(df_authors['MAGAID'])]
    
    df_merged = df_authors.merge(df_numcitations, on='MAGAID', how='left')
    
    df_merged = df_merged[df_merged['cumCitationsYear']\
                            .le(df_merged['RetractionYear'])]
    
    df_merged = df_merged.sort_values(by='cumCitationsYear', ascending=False)
    
    df_merged = df_merged.drop_duplicates(subset=['MAGAID'], keep='first')
    
    df_merged = df_merged.drop(columns=['RetractionYear'])
    
    return df_merged

def extract_numCollaborators(df_authors, indir):
    df_authors = df_authors[['MAGAID', 'RetractionYear']].drop_duplicates()
    
    df_numcollaborators = pd.read_csv(indir+"AID_year_numCollaborators.csv",
                            usecols=['AID', 'PubYear', 'cumCollaborators'])\
                                .rename(columns={'PubYear':'cumCollaboratorsYear',
                                                'AID': 'MAGAID'})
    
    df_numcollaborators = df_numcollaborators[df_numcollaborators['MAGAID'].isin(df_authors['MAGAID'])]
    
    df_merged = df_authors.merge(df_numcollaborators, on='MAGAID', how='left')
    
    df_merged = df_merged[df_merged['cumCollaboratorsYear']\
                            .le(df_merged['RetractionYear'])]
    
    df_merged = df_merged.sort_values(by='cumCollaboratorsYear', ascending=False)
    
    df_merged = df_merged.drop_duplicates(subset=['MAGAID'], keep='first')
    
    df_merged = df_merged.drop(columns=['RetractionYear'])
    
    return df_merged

def extract_firstAff(df_authors, indir):
    return df_authors[['MAGAID']].drop_duplicates()

def extract_rAff(df_authors, indir):
    df_authors = df_authors[['MAGAID', 'RetractionYear']].drop_duplicates()
    df_affrank = pd.read_csv(indir+"PID_AID_AffID_year_rank.csv",
                            usecols=['AID','year','AffID','rank'])\
                    .rename(columns={'year':'AffYear',
                                    'AID': 'MAGAID',
                                    'rank': 'AffRank'})
    # filtering
    df_affrank = df_affrank[df_affrank['MAGAID'].isin(df_authors['MAGAID'])]
    df_merged = df_authors.merge(df_affrank, on='MAGAID', how='left')
    df_merged = df_merged[~df_merged['AffYear'].isna() & 
                        ~df_merged['AffID'].isna()]
    
    df_merged = df_merged[df_merged['AffYear'].le(df_merged['RetractionYear'])]
    
    df_merged = df_merged.sort_values(by='AffYear')
    
    latest_aff_years = df_merged.groupby('MAGAID')['AffYear'].max()
    
    df_merged = pd.merge(df_merged, latest_aff_years, 
                            on=['MAGAID', 'AffYear'])
    
    return df_merged[['MAGAID','AffID','AffRank','AffYear']]\
            .drop_duplicates()

def main():
    # Defining paths 
    # Will need to fix this later to hide paths
    indir = "/scratch/sm9654/retraction_openalex/data/processed/"
    indir2 = "/scratch/sm9654/data/MAG_2021/derived/"
    indir3 = "/scratch/bka3/MAG_2021/"
    indir4 = "/scratch/sm9654/retraction_openalex/data/retraction_watch/"
    indir5 = "/scratch/sm9654/retraction_openalex/data/processed/RW_authors_w_fields/"
    
    df_authors = pd.read_csv(indir + "RWMAG_authors_SingleAndRepeatedOffendersSameYear.csv")
    
    magpids = df_authors['RetractedPaperMAGPID'].unique()
    magaids = df_authors['MAGAID'].unique()
    
    # Computing paper level features
    # extracting venues
    df_venues = extract_venue(magpids, indir3)          
    # extracting reason
    df_reason = extract_reason(df_authors, indir)
    # extracting num authors
    df_numauthors = extract_num_authors_rw(df_authors, indir4)
    # Merging paper features into df_authors
    df_authors = df_authors.merge(df_venues, on='RetractedPaperMAGPID', how='left')\
                            .merge(df_reason, on='RetractedPaperMAGPID', how='left')\
                            .merge(df_numauthors, on='RetractedPaperMAGPID', how='left')
    
    print(df_authors.columns)
    print(df_authors)

    # Computing author level features
    # extract gender
    df_gender = extract_gender(magaids, indir2)
    # extract author order
    df_order = extract_author_order(magaids, indir)
    # extract num papers
    df_numPapers = extract_numPapers(df_authors, indir2)
    # extract num citations
    df_numCitations = extract_numCitations(df_authors, indir2)
    # extract num collaborators
    df_numCollaborators = extract_numCollaborators(df_authors, indir2)
    # extract first affiliation rank
    # df_firstAff = extract_firstAff(df_authors, indir2)
    # extract retraction year affiliation rank
    df_rAff = extract_rAff(df_authors, indir2)
    # extract first pub year/age
    df_age = extract_age(df_authors, indir2)
    # extract disciplines
    df_disciplines = extract_disciplines(indir5)
    
    # merging
    df_authors = df_authors.merge(df_gender, on='MAGAID', how='left')\
                            .merge(df_order, on='MAGAID', how='left')\
                            .merge(df_age, on='MAGAID', how='left')\
                            .merge(df_numPapers, on='MAGAID', how='left')\
                            .merge(df_numCitations, on='MAGAID', how='left')\
                            .merge(df_numCollaborators, on='MAGAID', how='left')\
                            .merge(df_rAff, on='MAGAID', how='left')\
                            .merge(df_disciplines, on='MAGAID', how='left')
    
    df_authors.to_csv(indir+"authors_w_confounders.csv", 
                        index=False)
    
if __name__ == "__main__":
    main()