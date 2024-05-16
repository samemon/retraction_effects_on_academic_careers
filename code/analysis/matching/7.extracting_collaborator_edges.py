"""
This program will be used to extract 
1D and 2D collaborator edges for the given 
matched dataset of authors.
"""

# Importing relevant libraries
import pandas as pd
import sys
import numpy as np

# Let us read the matched RW dataset

#outdir = "../../data/main/"
#outdir = "/scratch/sm9654/retraction_effects_on_collaboration_networks/data/main/rematching/"
#outdir = "/scratch/sm9654/retraction/RW_authors/rematched_pairs/parallel/150123/"
#fin = "../../data/main/RWMatched_filterUntilBulk.csv"
#fin = "../../data/main/RWMatched_intersection.csv"
# fin = "/scratch/sm9654/retraction/RW_authors/rematched_pairs/parallel/090323/RWMatched_intersection_woPapersCitationsCollaborators.csv"


# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"



df1_rw_matched = pd.read_csv(OUTDIR+"/RWMatched_intersection_woPapersCitationsCollaborators.csv",
                        usecols=['MAGPID','Record ID','MAGAID','MatchMAGAID']).\
                                drop_duplicates()

# ## PREPROCESSING THE RW MATCHED DATASET ##

# Extracting retracted scientists
df2_treated_scientists = df1_rw_matched[['MAGAID']].drop_duplicates()
df2_treated_scientists['ScientistType'] = 'retracted'

# Extracting matched scientists
df3_control_scientists = df1_rw_matched[['MatchMAGAID']].drop_duplicates()
df3_control_scientists['ScientistType'] = 'matched'
# Renaming the column
df3_control_scientists = df3_control_scientists.rename(columns={'MatchMAGAID':'MAGAID'})

# Merging the two types of scientists
df4_scientists = pd.concat([df2_treated_scientists,df3_control_scientists])

# Now let us read the file from which we will extract collaborations
df5_pid_aid_year = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
        usecols=['PID','AID','year']).\
        drop_duplicates().\
        rename(columns={'PID':'MAGPID',
                        'AID':'MAGAID',
                        'year':'MAGCollaborationYear'})


# Now let us do first merging to extract PIDs of papers that each scientist has worked on
df6_merged_onAID = df4_scientists.merge(df5_pid_aid_year, on='MAGAID',
                                        how='left')

# Now for each PID, let us extract all the authors.
df7_merged_onPID = df6_merged_onAID.merge(df5_pid_aid_year, on='MAGPID',
                                        how='left')

# Now let us rename the columns
df8_merged_onPID = df7_merged_onPID.rename(columns={'MAGAID_x':'MAGAID',
                                                'MAGAID_y':'MAGCollabAID',
                                                'MAGCollaborationYear_x':'MAGCollaborationYear'}).\
                                drop(columns=['MAGCollaborationYear_y','MAGPID'])

# Now let us remove all rows where MAGAID and MAGCollabAID are the same
df9_1d_collaborators = df8_merged_onPID[~df8_merged_onPID.MAGAID.\
        eq(df8_merged_onPID.MAGCollabAID)].drop_duplicates()

df9_1d_collaborators.to_csv(OUTDIR+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaborators.csv", 
                        index=False)
