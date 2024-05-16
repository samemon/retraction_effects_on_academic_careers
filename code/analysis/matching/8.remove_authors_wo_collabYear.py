"""
This program will be used 
to create a list of authors 
for which the collaboration 
year is missing for atleast 
1 collaborator. We shall later 
remove these as we can't compute 
the outcome for these authors. 

We will also remove all 
collaborations after 2021

We will then use that list to 
remove authors and corresponding 
matches where collaboration year is 
missing. We will also remove all 
matches without collaboration year 
and their corresponding treatment if 
the match removed was the only match. 


Finally we will create two new files: 
1) Without invalid authors from the matched 
sample.
2) A 1D collaborators file w/o 2021 year 
and w/o invalid authors.
"""

# Importing relevant libraries

import pandas as pd
import sys
import numpy as np

# Fixing a random state
np.random.seed(3)

# Fixing the float to 1 decimal place
pd.options.display.float_format = '{:.2f}'.format

# INDIR = "/scratch/sm9654/retraction_effects_on_collaboration_networks/data/main/rematching/"
# OUTDIR = "/scratch/sm9654/retraction_effects_on_collaboration_networks/data/main/"
# OUTDIR2 = "/scratch/sm9654/retraction/RW_authors/rematched_pairs/parallel/090323/"

# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"



def get_stats(df_matched, key=['Record ID','MAGAID']):

        grouped_size = df_matched.groupby(key).size()
        match_grouped_size = df_matched.groupby(key)['MatchMAGAID'].nunique()

        print("Total number of papers matched", df_matched['Record ID'].nunique())

        print("Authors matched",df_matched[~df_matched.MatchMAGAID.isna()].MAGAID.nunique())

        print("Average number of matches", match_grouped_size.mean())

        print("Minimum number of matches", match_grouped_size.min())

        print("Maximum number of matches", match_grouped_size.max())


# Creating list of invalid authors
df_1d_collabs = pd.read_csv(OUTDIR+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaborators.csv")

magaids_wo_collabYear = df_1d_collabs[df_1d_collabs.MAGCollaborationYear.isna()].MAGAID.unique().tolist()

with open(OUTDIR+"/magaids_wo_collabYear.txt", 'w') as fp:
        for magaid in magaids_wo_collabYear:
                # write each item on a new line
                fp.write("%s\n" % magaid)
        
# Reading the partially matched file.
df_matched1 = pd.read_csv(OUTDIR+"/RWMatched_intersection_woPapersCitationsCollaborators.csv")

get_stats(df_matched1)
print("Number of MAGAIDs,Matches wo papers citations,", df_matched1.MAGAID.nunique(),
        df_matched1.MatchMAGAID.nunique())

df_matched2 = pd.read_csv(OUTDIR+"/RWMatched_intersection_w_PapersCitationsCollaboratorsAtRetraction.csv").\
                drop(columns=['RetractionYear'])
                
print("Number of MAGAIDs,Matches w/ papers citations,", df_matched2.MAGAID.nunique(),
        df_matched2.MatchMAGAID.nunique())
                
df_matched = df_matched1.merge(df_matched2, on=['MAGAID','MatchMAGAID','Record ID', 'MAGPID'],
                        how='right')

print("Number of MAGAIDs,Matches w/ merged fields", df_matched.MAGAID.nunique(),
        df_matched.MatchMAGAID.nunique())

# Let us remove all authors that are invalid
df_matched_filtered = df_matched[~df_matched.MAGAID.isin(magaids_wo_collabYear) & 
                                ~df_matched.MatchMAGAID.isin(magaids_wo_collabYear)]

df_matched_filtered.to_csv(OUTDIR+"/RWMatched_intersection_wPapersCitationsCollaboratorsAtRetraction_wCollabYear.csv",
                        index=False)

get_stats(df_matched_filtered)

# Now let us remove invalid authors and rows from 1D collaborators file.


df_1d_collabs_valid = df_1d_collabs[df_1d_collabs.MAGCollaborationYear.le(2020) & 
                                ~df_1d_collabs.MAGAID.isin(magaids_wo_collabYear)]

df_1d_collabs_valid.to_csv(OUTDIR+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020.csv",
                        index=False)

