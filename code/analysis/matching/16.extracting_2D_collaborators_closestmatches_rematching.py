import pandas as pd
import sys
import numpy as np


# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"


# Reading files with closest match based on different thresholds
# Why? Because we will report results for all three distances 
# And 10% matches are not necessarily proper subset of 20% matches
# This is because distance is a function of papers, cites, collaborators
# And so a person matched on papers, does not necessarily get matched on citations
df1 = pd.read_csv(OUTDIR+"/closestAverageMatch_tolerance_0.1_w_0.8.csv",
                usecols=['MAGAID','MatchMAGAID']).drop_duplicates()

df2 = pd.read_csv(OUTDIR+"/closestAverageMatch_tolerance_0.2_w_0.8.csv",
                usecols=['MAGAID','MatchMAGAID']).drop_duplicates()

df3 = pd.read_csv(OUTDIR+"/closestAverageMatch_tolerance_0.3_w_0.8.csv",
                usecols=['MAGAID','MatchMAGAID']).drop_duplicates()

# Getting set of all control and treatment magaids
df3_magaids = set(df3.MAGAID.unique())
df_matchmagaids = set(df3.MatchMAGAID.unique()).\
                            union(set(df2.MatchMAGAID.unique())).\
                                union(set(df1.MatchMAGAID.unique()))
# merging control and treatment magaids
lst_magaids_closestmatch = df3_magaids.union(df_matchmagaids)

# Reading 1d collaborators
df9_1d_collaborators = pd.read_csv(OUTDIR+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020.csv")

# Only getting those rows where magaid matches what is relevant
df9_1d_collaborators = df9_1d_collaborators[df9_1d_collaborators.MAGAID.isin(lst_magaids_closestmatch)]

# Saving that at home as it should not take much space as these are 1d edges
df9_1d_collaborators.to_csv(OUTDIR+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020_closestMatch30_nonattrited_matches.csv",
                            index=False)

"""
Here, we are taking a de-tour and saving the file with confounders for those 
that were matched using closestMatch
"""

# dftemp = pd.read_csv("RWMatched_intersection_wPapersCitationsCollaboratorsAtRetraction_wCollabYear.csv")
# dftemp = dftemp[dftemp.MAGAID.isin(df3_magaids) & 
#                 dftemp.MatchMAGAID.isin(df_matchmagaids)]
# dftemp.to_csv("/scratch/sm9654/retraction/RW_authors/rematched_pairs/parallel/090323/RWMatched_intersection_wPapersCitationsCollaboratorsAtRetraction_wCollabYear_closestMatch30_nonattrited_matches.csv",
#             index=False)

# sys.exit()

# Now let us read the file from which we will extract collaborations
df5_pid_aid_year = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
        usecols=['PID','AID','year']).\
        drop_duplicates().\
        rename(columns={'PID':'MAGPID',
                        'AID':'MAGAID',
                        'year':'MAGCollaborationYear'})

# Now let us extract second degree collaborators
df10_1d_collabAIDs = df9_1d_collaborators[['MAGCollabAID']].drop_duplicates()

# Now let us do first merging to extract PIDs of papers that each scientist has worked on
df11_merged_onCollabAID = df10_1d_collabAIDs.merge(df5_pid_aid_year,
                                                left_on='MAGCollabAID',
                                                right_on='MAGAID',
                                                how='left').\
                                            drop(columns=['MAGAID'])


df12_merged_onPID = df11_merged_onCollabAID\
                        .merge(df5_pid_aid_year, on='MAGPID', how='left')

df13_merged_onPID = df12_merged_onPID.rename(columns={'MAGAID':'MAGCollab2AID',
                                            'MAGCollaborationYear_x':'MAGCollaboration2Year'}).\
                            drop(columns=['MAGCollaborationYear_y','MAGPID'])

df14_merged_filtered = df13_merged_onPID[~df13_merged_onPID.MAGCollabAID.\
    eq(df13_merged_onPID.MAGCollab2AID)]

df14_merged_filtered.to_csv(OUTDIR+\
        "/RW_MAGcollaborators_2ndDegree_rematchingAtRetraction_closestmatch_rematching_nonattrited_matches.csv", 
                    index=False)