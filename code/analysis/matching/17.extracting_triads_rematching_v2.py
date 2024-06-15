"""
This program will be used to extract the 
output for triadic closure. 
Difference between v1 and v2 is that 
v2 is not parallel, and will only run 
for closest matches. 

We will compute two things for our retracted 
scientists and matches:

1) Number of new collaborators that were part 
of completing a triad within the 5 year window. 

2) NC: Newman's global clustering coefficient i.e. 
number of triads closed post-retraction
divided by the number of open triads prior to retraction. 

Logic: 

1*) Load treatment and control with outcomes. Relevant columns are:
    (a) MAGAID (b) MatchMAGAID (c) RetractionYear ---> df_1

2*) Load the 1D collaborator file and filter out the MAGAID/MatchMAGAID 
    that are not in df_1  ---> df_2

3*) Load the 2D collaborator file --> df_3

4*) Merge the two dataframes (df_1 and df_2) and get the pre- and post- collaborators --> df_5

5*) Compute the pre- and post-collaborators as a set --> this is df_6

6*) Merge (MAGAID/MatchMAGAID from df_2) with 2d collaborator file (df_3) --> df_7

7*) Now merge df_6 and df_7 --> df_8

8*) Now compute the pre- and post-collaborators of 1d collaborators using RetractionYear

9*) Now compute the pre- and post-collaborators of 1d collaborators as a set

10*) Finally compute the intersection between pre-retraction collaborators 
of 1d collaborators and post-retraction new collaborators of scientists
"""

import pandas as pd
import numpy as np

import sys

# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"

OUTDIR_TC = OUTDIR+"/triadic_closure/"

if __name__ == "__main__":
    argv = sys.argv[1:]
    
    if(len(argv) != 1):
        # If less than or greater than 1 arguments, give error
        print("Usage: python3 extracting_triads_rematching.py <index>")
        sys.exit()

    number_of_split = int(argv[0])
    #scientistType = argv[1]
    print("Index value:", number_of_split)
    
    # Let us first read the filtered_sample file for reading RetractionYear
                                
    df_filtered = pd.read_csv(INDIR_PROCESSED+"/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections_Augmented.csv",
                        usecols=['Record ID','RetractionYear','AttritedClass',
                                'nRetracted']).drop_duplicates()
    
    df_filtered = df_filtered[df_filtered['nRetracted'].eq(1) & 
                            df_filtered['AttritedClass'].eq(0)]

    INDIR_matches = OUTDIR
    
    # Now let us read the MAGAIDs (and MatchMAGAIDs)
    df10_magaids = pd.read_csv(INDIR_matches+"/closestAverageMatch_tolerance_0.1_w_0.8.csv",
                            usecols=['MAGAID','MatchMAGAID','Record ID']).\
                                drop_duplicates().\
                                    merge(df_filtered, on='Record ID')
                                    
    df20_magaids = pd.read_csv(INDIR_matches+"/closestAverageMatch_tolerance_0.2_w_0.8.csv",
                            usecols=['MAGAID','MatchMAGAID','Record ID']).\
                                drop_duplicates().\
                                    merge(df_filtered, on='Record ID')
                                
    df30_magaids = pd.read_csv(INDIR_matches+"/closestAverageMatch_tolerance_0.3_w_0.8.csv",
                            usecols=['MAGAID','MatchMAGAID','Record ID']).\
                                drop_duplicates().\
                                    merge(df_filtered, on='Record ID')
    
    # Now we have list of all matched treatment and control alongside retraction year
    # Note: Control can be one to many i.e. one matched to multiple 
    # Note: This is now sorted in ascending order
    df_matched = pd.concat([df10_magaids[['MAGAID','RetractionYear']].drop_duplicates(),
                            df20_magaids[['MAGAID','RetractionYear']].drop_duplicates(),
                            df30_magaids[['MAGAID','RetractionYear']].drop_duplicates(),
                            df10_magaids[['MatchMAGAID','RetractionYear']].drop_duplicates().\
                                        rename(columns={'MatchMAGAID':'MAGAID'}),
                            df20_magaids[['MatchMAGAID','RetractionYear']].drop_duplicates().\
                                        rename(columns={'MatchMAGAID':'MAGAID'}),
                            df30_magaids[['MatchMAGAID','RetractionYear']].drop_duplicates().\
                                        rename(columns={'MatchMAGAID':'MAGAID'})]).\
                                            drop_duplicates()
    
    # At this point, we have all the magaids and matchmagaids for which we need to extract triads
    
    # Reading the 1d collaborators file and filtering out scientists not in our analysis
    df_1d_collaborators = pd.read_csv(OUTDIR+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020_closestMatch30_nonattrited_matches.csv")
    
    # This step is redundant yet let's do it for sanity check
    df_1d_collaborators_relevant = df_1d_collaborators[df_1d_collaborators['MAGAID'].isin(df_matched['MAGAID'].unique())]
    
    assert(df_1d_collaborators_relevant.shape[0] == df_1d_collaborators.shape[0])
    
    # Now let us merge collaborators with matched file to extract retraction year
    # This should have MAGAID, MAGCollabAID, Retraction year, collaboration year
    df_merged = df_matched.merge(df_1d_collaborators_relevant, on=['MAGAID'], how='left')
    
    # At this point, df_merged --may-- contains MAGAIDs without collaboration year
    # This is because they may have NO prior collaborators at all. 
    # OR it could be that these have no collaboration year for atleast one collaborator
    # If it is the former, we need to keep them, but if it is latter, it is invalid
    # As such let us remove those

    # Reading file with magaids without collaboration year
    df_invalid = pd.read_csv(OUTDIR+"/magaids_wo_collabYear.txt", 
                            sep="\t", header=None).\
                                rename(columns={0:'MAGAID'})
                                
    lst_magaids_invalid = df_invalid.MAGAID.unique().tolist()
    
    # Let us now remove these invalid magaids (from my checking, these are already removed -- still)
    # We also want to remove authors without any prior collaborators. 
    # As these authors will not have closed any triads
    # We won't remove these authors from analysis -- we will add them back later
    df_merged = df_merged[~df_merged.MAGAID.isin(lst_magaids_invalid) |
                        ~df_merged.MAGCollaborationYear.isna()].\
                            sort_values(by='MAGAID')
    
    # NOTE: CHECKED THE CODE AT THIS POINT. LOOKS GOOD!
    
    # Defining the number of splits
    n = 100 # number of splits
    
    # Getting all magaids as a list (sorted list)
    lst_valid_magaids = df_merged.MAGAID.unique().tolist()
    
    # Splitting the list into n splits
    splits = np.array_split(lst_valid_magaids, n)
    
    # Getting the nth split of magaids
    nth_split = splits[number_of_split]
    
    print("Number of MAGAIDs being processed", len(nth_split))
    
    # Splitting the magaids into n chunks
    df_merged_n = df_merged[df_merged.MAGAID.isin(nth_split)].copy()
    
    print(df_merged_n.shape[0], df_merged_n.MAGAID.nunique())
    
    #Let us first create a prepost flag to check if a collaborator is before or after retraction

    def get_prepost_flag(row):
        if(row['MAGCollaborationYear'] <= row['RetractionYear']):
            return 'pre'
        else:
            if((row['MAGCollaborationYear']-row['RetractionYear'])<=5):
                return 'post5'
            else:
                return 'post'
    
    # Now let us apply the get_prepost_flag function to each row for treatment and control
    # This will tell us whether collaboration was before or after 
    df_merged_n['PrePostFlag5'] = df_merged_n.apply(lambda row: get_prepost_flag(row), 
                                                        axis=1)
    
    # Now let us extract pre- and post-retraction collaborators with a 5 year window as set

    # Grouping by MAGAID, gender, and pre-post flag to extract pre, and post- re. collabs.
    
    df_merged_w5 = df_merged_n.groupby(['MAGAID','RetractionYear','PrePostFlag5'])\
                                ['MAGCollabAID'].apply(set).unstack().reset_index().\
                                drop(columns=['post'])
    # Dealing with NaNs, and replacing them with empty set

    # For treatment
    df_merged_w5['pre'] = df_merged_w5['pre'].apply(lambda d: d if isinstance(d, set) else set())
    df_merged_w5['post5'] = df_merged_w5['post5'].apply(lambda d: d if isinstance(d, set) else set())
    
    # Extracting set of new connections post-retraction
    # At this point, this dataframe contains MAGAID, RetractionYear, post5 and pre as columns
    # And now it also contains CollabAIDGainedW5 as the column
    # This dataframe we save for now, and we shall use it later. 
    df_merged_w5['CollabAIDGainedW5'] = df_merged_w5.apply(lambda row: row['post5']-row['pre'], 
                                                                axis=1)
    
    
    # Now let us create another dataframe for 2d collaborators
    # Before that, let us read only magaid collaborators that are relevant
    lst_1d_collaborators_n = df_1d_collaborators_relevant[df_1d_collaborators_relevant.MAGAID.isin(nth_split)]\
                                    .MAGCollabAID.unique().tolist()
    
    # Now we need to read 2d collaborators
    path_2dcollabs = OUTDIR+\
                    "/RW_MAGcollaborators_2ndDegree_rematchingAtRetraction_closestmatch_rematching_nonattrited_matches.csv"
    
    df_2d_collaborators = pd.read_csv(path_2dcollabs)
    
    # Only getting relevant 2d collaborators
    df_2d_collaborators_n = df_2d_collaborators[df_2d_collaborators.MAGCollabAID.\
                                            isin(lst_1d_collaborators_n)]
    
    
    # Now we need to merge MAGAID and retraction year from df_merged_n
    df_merged_wCollabAIDs = df_merged_w5[['MAGAID','RetractionYear']].drop_duplicates()\
                                        .merge(df_merged[['MAGAID','MAGCollabAID','RetractionYear']]\
                                                        .drop_duplicates(),
                                                        on=['MAGAID','RetractionYear'])
    
    # Merging to get 2d Collaborators
    # This will have 5 columns: MAGCollabAID, MAGCollab2AID, MAGCollaborationYear, MAGAID, RetractionYear
    
    df_merged_w2dCollabs = df_2d_collaborators_n.merge(df_merged_wCollabAIDs, on='MAGCollabAID')

    # Now we need to first check if the collaboration was pre or post retraction

    def get_prepost_flag2D(row):
        if(row['MAGCollaboration2Year'] <= row['RetractionYear']):
            return 'pre_2D'
        else:
            if(row['MAGCollaboration2Year']-row['RetractionYear']<=5):
                return 'post5_2D'
            else:
                return 'post_2D'
    
    # Now let us apply the get_prepost_flag function to each row for treatment and control

    df_merged_w2dCollabs['PrePostFlag5_2D'] = df_merged_w2dCollabs.apply(lambda row: get_prepost_flag2D(row), 
                                                    axis=1)

    print("Checkpoint 2")
    
    # Now we don't quite care about post5_2D and post_2D, so let us only keep pre_2D
    df_merged_w2dCollabs = df_merged_w2dCollabs[df_merged_w2dCollabs['PrePostFlag5_2D']=='pre_2D']

    # Getting hashable version of the above two dataframes
    
    df_hashable = df_merged_w2dCollabs[['MAGAID','MAGCollabAID',
                                    'RetractionYear','PrePostFlag5_2D','MAGCollab2AID']].\
                            drop_duplicates()
                            
    # Grouping by MAGAID, gender, and pre-post flag to extract pre, and post- re. collabs.
    df_merged_w5_2D = df_hashable.groupby(['MAGAID','MAGCollabAID',
                                                    'RetractionYear','PrePostFlag5_2D'])\
                            ['MAGCollab2AID'].apply(set).unstack().reset_index()
    
    print("Checkpoint 3")
    
    df_merged_w5_2D['pre_2D'] = df_merged_w5_2D['pre_2D'].apply(lambda d: d if isinstance(d, set) else set())
    
    # At this point we have 2d merged dataframe.
    # We already have df_merged_w5. So we need to merge that to df_merged_w5_2D
    df_final = df_merged_w5[['MAGAID','CollabAIDGainedW5','pre','RetractionYear']].drop_duplicates(subset=['MAGAID','RetractionYear']).\
                    merge(df_merged_w5_2D[['MAGAID','pre_2D','RetractionYear']].drop_duplicates(subset=['MAGAID','RetractionYear']),
                        on=['MAGAID','RetractionYear'], how='left')
    
    print("Number of MAGAIDs for computing triads", df_final.MAGAID.nunique())
    
    df_final['pre_2D_wo_1D'] = df_final.apply(lambda row: row['pre_2D']-row['pre'], axis=1)        
    
    print("Checkpoint 4")
    df_final['TriadsClosed'] = df_final.apply(lambda row: row['pre_2D_wo_1D'].\
                                                    intersection(row['CollabAIDGainedW5']), 
                                                            axis=1)
    print("Checkpoint 5")
    #df_final = df_final.drop(columns=['CollabAIDGainedW5'])

    # Extracting number of open triads before retraction
    df_final['NumOpenTriads'] = df_final.apply(lambda row: len(row['pre_2D_wo_1D']), axis=1)

    df_final['NumTriadsClosed'] = df_final.apply(lambda row: \
            len(row['TriadsClosed']), axis=1)

    ## Computing Newman's Clustering Coefficient
    df_final['NC'] = df_final['NumTriadsClosed']/df_final['NumOpenTriads']
    
    print("Number of MAGAIDs being saved", df_final.MAGAID.nunique())

    df_final.to_csv(OUTDIR_TC\
                        +"/RWMatched_"+str(number_of_split)+"_of100_wTriads.csv", index=False)