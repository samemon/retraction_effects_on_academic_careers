"""
In this piece of code, we shall 
extract the characteristics of 
1d collaborators at the time of 
retraction as well as at the time 
of collaboration year.

For each (CollabAID, MAGAID, CollabYear) tuple, 
the characteristics we shall extract 
will be the following:

1. CollabGender

2. CollabAcademicAgeAtRetraction
3. CollabAcademicAgeAtCollabYear

4. CollabNumPapersAtRetraction
5. CollabNumPapersAtCollabYear

6. CollabNumCitationsAtRetraction
7. CollabNumCitationsAtCollabYear

8. CollabNumCollaboratorsAtRetraction
9. CollabNumCollaboratorsAtCollabYear

10. CollabAffiliationRankAtRetraction
11. CollabAffiliationRankAtCollabYear

"""

import pandas as pd
import numpy as np
import sys

# Let us first read the treatment and control files for matching
# These will be used for filtering out collaborators that are not relevant

# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where match files are
INDIR_MATCHING = INDIR_PROCESSED + "/author_matching/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/collaborator_quality_analysis/"

def read_gender_firstyear_for_collabs():

    # let us load the first pub year file and rename
    df_firstyear = pd.read_csv(INDIR_DERIVED+"/AID_PID_firstPubYear.csv",
                                    usecols=['AID','year'])\
                                            .rename(columns={'AID':'MAGCollabAID',
                                                    'year':'CollabMAGFirstPubYear'})\
                                            .drop_duplicates()
    
    print("loaded first pubyear file")

    # Let us first load the MAG gender file
    df_gender = pd.read_csv(INDIR_DERIVED+"/AID_firstname_gender.csv",
                                    usecols=['AID','gender','confidence'])\
                                    .rename(columns={'AID':'MAGCollabAID',
                                                    'gender':'CollabGenderizeGender',
                                                    'confidence':'CollabGenderizeConfidence'})
    
    
    # Merging two dataframes
    df_gender_firstyear = df_gender.\
            merge(df_firstyear, on='MAGCollabAID', how='right')
    
    print("merged gender to first pub year")
    
    return df_gender_firstyear

def match_papers_cites_collabs(df_collabs, yearColumn, yearRoot='Retraction'):
    
    # First we need to load papers, citations, and collaborators for retracted authors
    
    # Loading papers file
    df_papers = pd.read_csv(INDIR_DERIVED+"/MAGAID_PubYear_numPapers_cumPapers_correctedPostNHB.csv",
                                usecols=['MAGAID','year','cumPapers']).\
                                    rename(columns={'MAGAID':'MAGCollabAID',
                                                    'year':'CollabMAGCumPapersYearAt'+yearRoot,
                                                    'cumPapers':'CollabMAGCumPapersAt'+yearRoot})
    
    print("Data type for AID:", df_papers.MAGCollabAID.dtype)
    print("Data type for Year:", df_papers['CollabMAGCumPapersYearAt'+yearRoot].dtype)
    
    # Loading the citations first
    df_citations = pd.read_csv(INDIR_DERIVED+"/AID_year_newCitations_cumCitations_corrected_collaborators.csv",
                                usecols=['MAGAID','PID_PubYear','cumCitations']).\
                                    rename(columns={'MAGAID':'MAGCollabAID',
                                                    'PID_PubYear':'CollabMAGCumCitationsYearAt'+yearRoot,
                                                    'cumCitations':'CollabMAGCumCitationsAt'+yearRoot})
                                    
    print("Data type for AID:", df_citations.MAGCollabAID.dtype)
    print("Data type for Year:", df_citations['CollabMAGCumCitationsYearAt'+yearRoot].dtype)
    
    # Loading collaborators
    df_collaborators = pd.read_csv(INDIR_DERIVED+"/AID_year_numCollaborators.csv",
                                usecols=['AID','PubYear','cumCollaborators']).\
                                    rename(columns={'AID':'MAGCollabAID',
                                                    'PubYear':'CollabMAGCumCollaboratorsYearAt'+yearRoot,
                                                    'cumCollaborators':'CollabMAGCumCollaboratorsAt'+yearRoot})
                                    
    print("Data type for AID:", df_collaborators.MAGCollabAID.dtype)
    print("Data type for Year:", df_collaborators['CollabMAGCumCollaboratorsYearAt'+yearRoot].dtype)
    
    #Now we shall merge this with papers, citations, collaborators
    
    # PROCESSING PAPERS FIRST
    # Merging papers with df_collabs
    df_merged_papers = df_collabs.merge(df_papers, on='MAGCollabAID', how='left')
    
    # Removing all entries where year of cumulative papers is more than year of collab/retrac
    # sorting by year of papers and selecting the last entry (closest to collab/retr)
    
    df_merged_papers = df_merged_papers[df_merged_papers\
                                ['CollabMAGCumPapersYearAt'+yearRoot].le(df_merged_papers[yearColumn])].\
                                    sort_values(by='CollabMAGCumPapersYearAt'+yearRoot).\
                                        drop_duplicates(subset=['MAGAID','MAGCollabAID',
                                                                'RetractionYear','MAGCollaborationYear'], keep='last')
                                        
    
    # PROCESSING CITATIONS
    
    
    df_merged_citations = df_collabs.merge(df_citations, on='MAGCollabAID', how='left')
    
    # Removing all entries where year of cumulative citations is more than year of collab/retrac
    # sorting by year of citations and selecting the last entry (closest to collab/retr)
    df_merged_citations = df_merged_citations[df_merged_citations\
                                ['CollabMAGCumCitationsYearAt'+yearRoot].le(df_merged_citations[yearColumn])].\
                                    sort_values(by='CollabMAGCumCitationsYearAt'+yearRoot).\
                                        drop_duplicates(subset=['MAGAID','MAGCollabAID',
                                                                'RetractionYear','MAGCollaborationYear'], keep='last')
    
    df_merged_collaborators = df_collabs.merge(df_collaborators, on='MAGCollabAID', how='left')
    
    # Removing all entries where year of cumulative collabs is more than year of collab/retrac
    # sorting by year of collabs and selecting the last entry (closest to collab/retr)
    
    df_merged_collaborators = df_merged_collaborators[df_merged_collaborators\
                                ['CollabMAGCumCollaboratorsYearAt'+yearRoot].le(df_merged_collaborators[yearColumn])].\
                                    sort_values(by='CollabMAGCumCollaboratorsYearAt'+yearRoot).\
                                        drop_duplicates(subset=['MAGAID','MAGCollabAID',
                                                                'RetractionYear','MAGCollaborationYear'], keep='last')
                                        
    # Now let us merge all three dataframes
    default_cols = ['MAGAID','MAGCollabAID','RetractionYear',
                    'MAGCollaborationYear','ScientistType',
                    'CollabGenderizeGender','CollabGenderizeConfidence',
                    'CollabMAGFirstPubYear']
    
    # Why merge on these columns? Because we need the number of papers, cites
    # and collabs for each collaborator at the time of each collaboration for 
    # the particular magaid with the given retraction year
    
    df_merged_all = df_collabs[default_cols].merge(df_merged_papers, on=default_cols, how='left')\
                                            .merge(df_merged_citations, on=default_cols, how='left')\
                                            .merge(df_merged_collaborators, on=default_cols, how='left')

    # Now we impute NaNs with 0s.
    df_merged_all['CollabMAGCumPapersAt'+yearRoot] = df_merged_all['CollabMAGCumPapersAt'+yearRoot].fillna(0)
    df_merged_all['CollabMAGCumCitationsAt'+yearRoot] = df_merged_all['CollabMAGCumCitationsAt'+yearRoot].fillna(0)
    df_merged_all['CollabMAGCumCollaboratorsAt'+yearRoot] = df_merged_all['CollabMAGCumCollaboratorsAt'+yearRoot].fillna(0)
    
    # Printing the shape
    print(df_merged_all.shape, df_collabs.shape)
    
    return df_merged_all


df_treatment = pd.read_csv(INDIR_MATCHING+"/RWMAG_rematched_treatment_augmented_rematching_30perc.csv",
                        usecols=['MAGAID',
                                'RetractionYear']).drop_duplicates()

df_control = pd.read_csv(INDIR_MATCHING+"/RWMAG_rematched_control_augmented_rematching_30perc.csv",
                        usecols=['MatchMAGAID',
                                'RetractionYear']).drop_duplicates().\
                                rename(columns={'MatchMAGAID':'MAGAID'})

df_treatment_control = pd.concat([df_treatment, df_control])

# Now we read 1d collaborators
df_1d_collaborators = pd.read_csv(INDIR_MATCHING+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020_30perc.csv")\
                .drop(columns=['FirstYearPostRetraction',
                                'YearOfAttrition', 'AuthorType',
                                'YearsBetweenRyearAndFirstActivityPostRetraction',
                                ])

df_1d_collaborators_relevant = df_1d_collaborators\
                [df_1d_collaborators.MAGAID.isin(df_treatment_control.MAGAID.unique()) & 
                df_1d_collaborators.MAGCollaborationYear.le(2020)]

print("After removing irrelevant collaborators")

print(df_1d_collaborators_relevant.MAGAID.nunique(),
        df_1d_collaborators_relevant.MAGCollabAID.nunique(),
        df_1d_collaborators_relevant.shape[0])

# Let us now augment retraction year (the dataframe size may increase if 1-many matches)
df_1d_collaborators_relevant = df_1d_collaborators_relevant\
                                .merge(df_treatment_control, on=['MAGAID','RetractionYear'])\
                                .drop_duplicates()

print("After merging with retraction year")

print(df_1d_collaborators_relevant.MAGAID.nunique(),
        df_1d_collaborators_relevant.MAGCollabAID.nunique(),
        df_1d_collaborators_relevant.shape[0])

# Let us now extract gender and first pub year
df_gender_firstyear = read_gender_firstyear_for_collabs()

df_1d_collaborators_wfirstyear = df_1d_collaborators_relevant\
                                .merge(df_gender_firstyear, on='MAGCollabAID',
                                        how='left')

# df_1d_collaborators_wfirstyear = df_1d_collaborators_relevant.copy().head(1000)

print("After merging with gender and firstpubyear")

print(df_1d_collaborators_wfirstyear.MAGAID.nunique(),
        df_1d_collaborators_wfirstyear.MAGCollabAID.nunique(),
        df_1d_collaborators_wfirstyear.shape[0])

print("Columns:",df_1d_collaborators_wfirstyear.columns)

# Extracting experience fields at the time of collaboration

df_1dcollabs_wExperienceFields_atCollab = \
        match_papers_cites_collabs(df_1d_collaborators_wfirstyear, 
                                'MAGCollaborationYear', yearRoot='Collaboration')

# Extracting experience fields at the time of retraction

df_1dcollabs_wExperienceFields_atRetraction = \
        match_papers_cites_collabs(df_1d_collaborators_wfirstyear, 
                                'RetractionYear', yearRoot='Retraction')

# merging both the dataframes

df_merged = df_1dcollabs_wExperienceFields_atRetraction.\
                merge(df_1dcollabs_wExperienceFields_atCollab,
                    on=['MAGAID','MAGCollabAID','RetractionYear',
                        'ScientistType',
                        'MAGCollaborationYear',
                        'CollabMAGFirstPubYear',
                        'CollabGenderizeGender',
                        'CollabGenderizeConfidence'])
                
df_merged.to_csv(OUTDIR+"/1Dcollaborators_for_matched_sample_30.csv", index=False)

print("Done!")