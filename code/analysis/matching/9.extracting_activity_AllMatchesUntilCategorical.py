"""
In this code, we shall compute post-retraction activity for 
retracted authors matches. This is so that we can remove authors and 
matches who have been attrited post the year of retraction to avoid survivorship 
bias. 

We will do the following:
1. First we shall read PID_AID_.. file to read activity for whole mag

2a. We will only retrieve activity of authors that are in matches or retracted
2b. We will only retrieve activity until 2020
2c. We will only retrieve activity with publication year

3a. We will now read attrition year for each author 
3b. We will filter attrition year for only relevant authors
3c. We will replace -1 with 2020

4. Now we shall merge the two dataframes. 

5. We shall remove all activity post attrition

6. Now we shall identify the LatestActivityYear for each author

7. Now we shall compute the YearsActive for each author. Each author should 
have a YearsActive and a LatestActivityYear. 

8. Now we filter authors and matches if YearsActive <= 0.  

9a. Now we check how many retracted authors are still left. 
9b. We also check how many matches are left
9c. We also check how many pairs are left i.e. retracted authors and matches 
and compare it to how many we had prior to filtering. 

10. We save the YearsActive file as well as the authors-matches file. 
"""

import pandas as pd

# INDIR = "/scratch/sm9654/retraction/RW_authors/rematched_pairs/parallel/090323/"
# OUTDIR = "/home/sm9654/retraction_effects_on_collaboration_networks/data/main/"

# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"


def extract_activity(df, lst_scientists, key, root):

    # 1. Now let us read the file from which we will extract activity
    df_pid_aid_year = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
            usecols=['AID','year']).\
                drop_duplicates().\
                rename(columns={'AID':key,
                                'year':'MAGPubYear'})
    # 2. Only reading relevant activity
    df_pid_aid_year_relevant = df_pid_aid_year[df_pid_aid_year['MAGPubYear'].le(2020) & 
                                            df_pid_aid_year[key].isin(lst_scientists) & 
                                            df_pid_aid_year['MAGPubYear'].notna()]
    
    # 3. Reading attrition year file
    df_attrition = pd.read_csv(INDIR_DERIVED+"/AIDs_YearOfAttrition_95percentile_allauthors.csv")\
                        .rename(columns={'AID':'MAGAID'})
    
    # Getting only relevant authors
    df_attrition = df_attrition[df_attrition['MAGAID'].isin(lst_scientists)]
    # Replacing -1 with 2020
    df_attrition['YearOfAttrition'] = df_attrition['YearOfAttrition'].replace(-1,2020)

    # 4. Merging the two dataframes    
    df_merged = df_pid_aid_year_relevant.merge(df_attrition, on='MAGAID', how='left')
    
    # Everyone should have year of attrition - just checking.
    print("number of authors with no activity year (should be 0 though):",
        df_merged[df_merged['YearOfAttrition'].isna()]['MAGAID'].nunique())

    # 5. Removing all activity post attrition year
    # Why? Because once they are attrited per our definition, their activity does 
    # not count. 
    
    print("Number of authors pre filtering:", df_merged['MAGAID'].nunique())
    
    df_filtered = df_merged[df_merged['MAGPubYear']\
                                .le(df_merged['YearOfAttrition'])]
    
    # printing number of authors left. This should remove none though
    
    print("Number of authors not filtered (should be none):",
        df_filtered['MAGAID'].nunique())
    
    # Let us get the retraction year for each author now
    df_filtered_wRyear_authors = df_filtered.merge(df[['MAGAID','RetractionYear']].drop_duplicates(),
                                                on='MAGAID')
    
    df_filtered_wRyear_authors['AuthorType'] = 'retracted'
    
    df_filtered_wRyear_matches = df_filtered.merge(df[['MatchMAGAID','RetractionYear']].drop_duplicates(),
                                                left_on='MAGAID',
                                                right_on='MatchMAGAID')\
                                                .drop(columns=['MatchMAGAID'])
                                                
    df_filtered_wRyear_matches['AuthorType'] = 'matched'
                                                
    # concatenating
    df_filtered_wRyear = pd.concat([df_filtered_wRyear_authors, 
                                    df_filtered_wRyear_matches])
    
    # Let us see how many authors we have
    print("num authors before filtering", df_filtered_wRyear['MAGAID'].nunique())
    
    # Let us now extract activity only post retraction
    df_filtered_wRyear_postRetractionActivity = df_filtered_wRyear[df_filtered_wRyear['MAGPubYear']\
                                                    .gt(df_filtered_wRyear['RetractionYear'])]
    
    # Number of authors after filtering
    print("num authors after filtering for activity.. removed authors have no activity post-retraction:", 
            df_filtered_wRyear_postRetractionActivity['MAGAID'].nunique())
    
    # 6. Computing earliest activity post retraction year.  
    df_earliestActivityPostRetraction = df_filtered_wRyear_postRetractionActivity.sort_values(by='MAGPubYear').\
                                drop_duplicates(subset=[key],keep='first')\
                                    .rename(columns={'MAGPubYear':'FirstYearPostRetraction'})
    
    
    df_earliestActivityPostRetraction['YearsBetweenRyearAndFirstActivityPostRetraction'] = \
                df_earliestActivityPostRetraction['FirstYearPostRetraction']  - df_earliestActivityPostRetraction['RetractionYear']
    
    #9. Let us check some stats
    print("Retracted authors total:", df_earliestActivityPostRetraction[df_earliestActivityPostRetraction['AuthorType']\
                                            .eq('retracted')]['MAGAID'].nunique())
    print("Retracted authors filtered:", 
            df_earliestActivityPostRetraction[df_earliestActivityPostRetraction['AuthorType']\
                                            .eq('retracted') & 
                                            df_earliestActivityPostRetraction['YearsBetweenRyearAndFirstActivityPostRetraction'].isin(range(1,6))]['MAGAID'].nunique())
    
    print("Matched authors total:", df_earliestActivityPostRetraction[df_earliestActivityPostRetraction['AuthorType']\
                                            .eq('matched')]['MAGAID'].nunique())
    print("Matched authors filtered:", 
            df_earliestActivityPostRetraction[df_earliestActivityPostRetraction['AuthorType']\
                                            .eq('matched') & 
                                            df_earliestActivityPostRetraction['YearsBetweenRyearAndFirstActivityPostRetraction'].isin(range(1,6))]['MAGAID'].nunique())
    
    # Checking how many author-match pairs are left. 
    print("Number of author-match pairs in total:", 
            df[['MAGAID','MatchMAGAID','RetractionYear']].drop_duplicates().shape[0])
    # Only getting survivors
    df_activity_filtered = df_earliestActivityPostRetraction[df_earliestActivityPostRetraction['YearsBetweenRyearAndFirstActivityPostRetraction'].isin(range(1,6))]
    # Let us remove retracted authors that did not survive from pairs
    df = df.merge(df_activity_filtered, on=['MAGAID','RetractionYear'])
    # Let us remove matched authors that did not survive from pairs
    df = df.merge(df_activity_filtered.rename(columns={'MAGAID':'MatchMAGAID'}), 
                on=['MatchMAGAID','RetractionYear'])
    # Printing how many survived
    print("Number of author-match pairs that survived:", 
            df[['MAGAID','MatchMAGAID','RetractionYear']].drop_duplicates().shape[0])
    
    df_earliestActivityPostRetraction.to_csv(OUTDIR+root+"_activity_post_retraction_rematching_forAllMatchedUntilCategorical.csv",
                    index=False)

df_control = pd.read_csv(OUTDIR+"RWMatched_intersection_wPapersCitationsCollaboratorsAtRetraction_wCollabYear.csv",
                        usecols=['MAGAID','MatchMAGAID','RetractionYear']).\
                            drop_duplicates()
                            
lst_matched_scientists = df_control.MatchMAGAID.unique().tolist() + \
                            df_control.MAGAID.unique().tolist()

extract_activity(df_control, lst_matched_scientists, 'MAGAID', 'treatmentcontrol')