"""
This code will create collaborator edges files 
for 30% threshold, 20% threshold, and 10% threshold so that we can copy that locally.
"""

import pandas as pd

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"

# Reading 1d collaborators file
fname = "RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020.csv"
df_collabs = pd.read_csv(OUTDIR+fname)

# Reading file that I processed for attrition year
df_attrition_year = pd.read_csv(OUTDIR+"treatmentcontrol_activity_post_retraction_rematching_forAllMatchedUntilCategorical.csv")

# Merging these two files
df_collabs_attrition = df_collabs.merge(df_attrition_year, on='MAGAID')

# Removing all collabs post attrition
df_collabs_attrition = df_collabs_attrition[df_collabs_attrition['MAGCollaborationYear'].le(df_collabs_attrition['YearOfAttrition'])]

# Only extracting collabs for 30%
df_30pc = pd.read_csv(OUTDIR+"/closestAverageMatch_tolerance_0.3_w_0.8.csv")

df_collabs_attrition = df_collabs_attrition[df_collabs_attrition['MAGAID'].isin(df_30pc['MAGAID']) | 
                                            df_collabs_attrition['MAGAID'].isin(df_30pc['MatchMAGAID'])]

df_collabs_attrition.to_csv(OUTDIR+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020_30perc.csv",
                            index=False)



# Only extracting collabs for 20%
df_20pc = pd.read_csv(OUTDIR+"/closestAverageMatch_tolerance_0.2_w_0.8.csv")

# Merging these two files
df_collabs_attrition = df_collabs.merge(df_attrition_year, on='MAGAID')

df_collabs_attrition = df_collabs_attrition[df_collabs_attrition['MAGAID'].isin(df_20pc['MAGAID']) | 
                                            df_collabs_attrition['MAGAID'].isin(df_20pc['MatchMAGAID'])]

df_collabs_attrition.to_csv(OUTDIR+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020_20perc.csv",
                            index=False)


# Only extracting collabs for 10%
df_10pc = pd.read_csv(OUTDIR+"/closestAverageMatch_tolerance_0.1_w_0.8.csv")

# Merging these two files
df_collabs_attrition = df_collabs.merge(df_attrition_year, on='MAGAID')

df_collabs_attrition = df_collabs_attrition[df_collabs_attrition['MAGAID'].isin(df_10pc['MAGAID']) | 
                                            df_collabs_attrition['MAGAID'].isin(df_10pc['MatchMAGAID'])]

df_collabs_attrition.to_csv(OUTDIR+"/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020_10perc.csv",
                            index=False)

# Now let us process other confounders
#df_confounders = pd.read_csv(OUTDIR+"/")

#df_confounders = df_confounders[df_confounders['MAGAID'].isin(df_30pc['MAGAID'])]

