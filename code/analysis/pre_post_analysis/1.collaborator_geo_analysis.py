"""
Purpose:

The purpose of this code is to 
extract the affiliations of new collaborators. 

Given a dataframe df with following columns:
MAGAID: ID of the author
CollabAID: ID of the collaborator
RetractionYear: year of retraction
MAGCollaborationYear: year of collaboration

1) For each author, we will first separate the 
collaborators into two groups: pre-retraction
and post-retraction based on the collaboration year. 
In other words, we will create a flag called 
"PreOrPost"

############################
# Filtering phase
2) Then for each collaborator of each author, 
we should identify the first year of collaboration, 
and we shall only keep that year. 

3) We shall now identify the post-retraction collaborators 
that are new, and remove them. 
###########################

4) We read and identify the affiliation of 
each collaborator in their first year of collaboration 
with the author (might be multiple affiliations)

5) We also get the affiliation of the author in the 
year of collaboration (might be multiple affiliations)

6) We identify the latitude and longitude for each 
affiliation based on affiliation id. 
"""

import pandas as pd
import sys

# Reading all the files
# Reading collaborators file
df_collaborators = pd.read_csv("/scratch/bka3/Retraction_data/processed/collaborators/RW_MAGcollaborators_1stDegree_rematching_woPapersCitationsCollaboratorsAtRetraction_wCollabYear_le2020_30perc.csv",
                            usecols=['MAGAID',
                                    'ScientistType',
                                    'MAGCollaborationYear',
                                    'MAGCollabAID',
                                    'RetractionYear',
                                    'YearOfAttrition'])\
                            .drop_duplicates()

# For each collaborator, let us take their first collab year with the author
df_collaborators['FirstCollaborationYear'] = df_collaborators\
                                .groupby(['MAGAID',
                                        'MAGCollabAID',
                                        'RetractionYear',
                                        'ScientistType'])['MAGCollaborationYear'].transform(min)


# Now we will only keep the first collaboration instance for each
# Btw this will also make sure that post retraction, we only have new collabs
df_collaborators = df_collaborators[df_collaborators['MAGCollaborationYear'] == df_collaborators['FirstCollaborationYear']]


# Now we put a pre-post flag

df_collaborators['RyearMinusCollabYear'] = df_collaborators['RetractionYear']-df_collaborators['MAGCollaborationYear']
df_collaborators['PrePostFlag'] = df_collaborators['RyearMinusCollabYear'].apply(lambda x: 'Pre' if x >= 0 else 'Post')

# Now we filter collaborators that occured post 5 years
df_collaborators = df_collaborators[df_collaborators['RyearMinusCollabYear'] >= -5]

# Let us now remove authors and matches that have no collaborators either pre- or post-
# This is because a comparison of distance cannot be made for such authors
# Create a pivot table to count Pre and Post
grouped = df_collaborators.groupby(['MAGAID', 'PrePostFlag'])\
                    ['MAGCollabAID'].nunique().reset_index(name='Count')

# pivot them as separate flags
pivot_table = grouped.pivot(index='MAGAID', 
                            columns='PrePostFlag', 
                            values='Count').fillna(0)

# rename columns
pivot_table = pivot_table.rename(columns={'Pre': 'Pre_Count', 'Post': 'Post_Count'})

# reset index to turn pivot_table into a regular dataframe
pivot_table.reset_index(inplace=True)

# merge with the original dataframe if necessary
df_merged = df_collaborators.merge(pivot_table, on='MAGAID', how='left')

# At this point, we are ready to get affiliations for each author
# Reading affiliations file
df_affiliations = pd.read_csv("/scratch/bka3/Retraction_data/MAG/derived/PID_AID_AffID_year_rank.csv",
                            usecols=['PID','AID','AffID','year','rank'])\
                            .rename(columns={'PID':'MAGAffPID',
                                            'AID':'MAGAID',
                                            'year':'MAGAffYear',
                                            'rank':'MAGAffRank',
                                            'AffID':'MAGAffID'})

# fltering to only include rows with relevant magaids
df_affiliations = df_affiliations[df_affiliations['MAGAID'].isin(df_merged['MAGAID']) | 
                                df_affiliations['MAGAID'].isin(df_merged['MAGCollabAID'])]

# Removing those without year or without aff id
df_affiliations = df_affiliations[~df_affiliations['MAGAffYear'].isna() & 
                                ~df_affiliations['MAGAffID'].isna()]


# Merging based on magaid first
df_merged_w_author_affiliations = df_merged.merge(df_affiliations,
                                                on='MAGAID',
                                                how='left')

# Removing all those with affiliations that are after collab year
df_merged_w_author_affiliations = df_merged_w_author_affiliations[df_merged_w_author_affiliations['MAGAffYear']\
                                    .le(df_merged_w_author_affiliations['MAGCollaborationYear'])]

df_merged_w_author_affiliations['MAGMaxAffYearByCollabYear'] = df_merged_w_author_affiliations\
                        .groupby(['MAGAID','MAGCollabAID'])['MAGAffYear'].transform('max')

# Finally let us remove all others and keep the max (this is interpolated affiliation)
df_merged_w_author_affiliations = df_merged_w_author_affiliations[df_merged_w_author_affiliations['MAGMaxAffYearByCollabYear'].\
                eq(df_merged_w_author_affiliations['MAGAffYear'])]\
                    .drop_duplicates()

# Now let us do the merging based on MAGCollabAID
# Renaming first
df_affiliations = df_affiliations.rename(columns={'MAGAffPID':'MAGCollabAffPID',
                                                'MAGAID':'MAGCollabAID',
                                                'MAGAffYear':'MAGCollabAffYear',
                                                'MAGAffRank':'MAGCollabAffRank',
                                                'MAGAffID':'MAGCollabAffID'})

# Filtering to only contain collab ids
df_affiliations = df_affiliations[df_affiliations['MAGCollabAID']\
        .isin(df_merged_w_author_affiliations['MAGCollabAID'])]

# Now we merge based on collabAID
df_merged_w_collab_aff = df_merged_w_author_affiliations.merge(df_affiliations,
                                                            on='MAGCollabAID',
                                                            how='left')

# Removing all those with affiliations that are after collab year
df_merged_w_collab_aff = df_merged_w_collab_aff[df_merged_w_collab_aff['MAGCollabAffYear']\
                                    .le(df_merged_w_collab_aff['MAGCollaborationYear'])]

df_merged_w_collab_aff['MAGMaxCollabAffYearByCollabYear'] = df_merged_w_collab_aff\
                        .groupby(['MAGAID','MAGCollabAID'])['MAGCollabAffYear'].transform('max')

# Finally let us remove all others and keep the max (this is interpolated affiliation)
df_merged_w_collab_aff = df_merged_w_collab_aff[df_merged_w_collab_aff['MAGMaxCollabAffYearByCollabYear'].\
                eq(df_merged_w_collab_aff['MAGCollabAffYear'])]\
                    .drop_duplicates()
        
# Reading geo file
df_geo = pd.read_csv("/scratch/bka3/Affiliation_Country_City_Geo_2.csv",
                    usecols=['AffiliationId',
                            'NormalizedName',
                            'Coordinates',
                            'Country',
                            'City'])\
                        .rename(columns={'AffiliationId':'MAGAffID',
                                        'NormalizedName':'MAGAffName',
                                        'Coordinates':'MAGAffLatLong',
                                        'Country':'MAGAffCountry',
                                        'City':'MAGAffCity'})\
                        .drop_duplicates()
                        
# Let us merge
df_merged_w_geo = df_merged_w_collab_aff.merge(df_geo, on=['MAGAffID'], how='left')

# Let us merge again
df_geo = df_geo.rename(columns={'MAGAffID':'MAGCollabAffID',
                                'MAGAffName':'MAGCollabAffName',
                                'MAGAffLatLong':'MAGCollabAffLatLong',
                                'MAGAffCountry':'MAGCollabAffCountry',
                                'MAGAffCity':'MAGCollabAffCity'})

df_merged_w_geo = df_merged_w_geo.merge(df_geo, on=['MAGCollabAffID'], how='left')

df_merged_w_geo.to_csv("/scratch/sm9654/retraction_openalex/data/processed/pre_post_analysis/collab_geo_prepost.csv",
                    index=False)
