"""
In this program we will do the 
following:

1. First we shall read all the authors and 
their matches.
2. Then we shall read every authors' first 
activity post retraction.
3. Third we shall remove authors that do not
have any activity within 5 years window post 
retraction
4. Next we shall address authors with 
multiple affiliations, and select 1 affiliation 
(top 1) for each author. We shall remove all matches 
as well. 
5. Finally, we shall save the file with author and matches 
for the next step to identify closest matches based on 
papers, citations and collaborators. 
"""

import pandas as pd
import sys

# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"

# Reading matching file
df_matched = pd.read_csv(OUTDIR+"/RWMatched_intersection_wPapersCitationsCollaboratorsAtRetraction_wCollabYear.csv")

# Reading activity file
df_activity = pd.read_csv(OUTDIR+"/treatmentcontrol_activity_post_retraction_rematching_forAllMatchedUntilCategorical.csv")

# Authors without 5 year window activity
df_activity = df_activity[df_activity['YearsBetweenRyearAndFirstActivityPostRetraction'].isin(range(1,6))]

print("Number of authors we have before filtering:", df_matched['MAGAID'].nunique())
print("Number of unique matched authors we have before filtering:", df_matched['MatchMAGAID'].nunique())

# Removing authors+matches without the activity post retraction
df_matched_filtered = df_matched[df_matched['MAGAID'].isin(df_activity['MAGAID']) & 
                                df_matched['MatchMAGAID'].isin(df_activity['MAGAID'])]

print("Number of authors we have after filtering:", df_matched_filtered['MAGAID'].nunique())
print("Number of unique matched authors we have after filtering:", df_matched_filtered['MatchMAGAID'].nunique())


# Now we need to deal with affiliations
# The problem is that authors can have multiple affiliations in the retraction year
# And we need to choose the one where the authors have the top affiliation. 
# But what is a top affiliation for an author?
# Since we have done so much filtering, we can't decide using df_matched
# Instead we must use the original filtered sample file to identify that

# The MAGRetractionYearAffRankOrdinal column already has top affiliation for each author
df_filtered_sample = pd.read_csv(INDIR_PROCESSED+"/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections_Augmented.csv",
                                usecols=['MAGAID','MAGRetractionYearAffRankOrdinal'])\
                                    .drop_duplicates()

# Now let us process the affiliation rank in df_matched_filtered

def make_rank_ordinal(rank):
    rank_dict = {'101-150':125,
                '151-200':175,
                '201-300':250,
                '301-400':350,
                '401-500':450,
                '501-600':550,
                '601-700':650,
                '701-800':750,
                '801-900':850,
                '901-1000':950,
                '1001-':1500}
    
    if rank in [str(rank) for rank in range(1,101)]:
        return int(rank)
    return rank_dict.get(rank)

df_matched_filtered['MAGRetractionYearAffRankOrdinal2'] = df_matched_filtered['MAGRetractionYearAffRank'].apply(make_rank_ordinal)

# Merging
df_merged = df_matched_filtered.merge(df_filtered_sample, on='MAGAID')

# Finally only extracting rows where Ordinal2 equal Ordinal
df_merged = df_merged[df_merged['MAGRetractionYearAffRankOrdinal'] == df_merged['MAGRetractionYearAffRankOrdinal2']]

# Removing redundant column
df_merged = df_merged.drop(columns=['MAGRetractionYearAffRankOrdinal2'])

print("Number of authors we have after processing multi aff:", df_merged['MAGAID'].nunique())
print("Number of unique matched authors we have after processing multi aff:", df_merged['MatchMAGAID'].nunique())

# Saving
df_merged.to_csv(OUTDIR+"RWMatched_intersection_wPapersCitationsCollaboratorsAtRetraction_wCollabYear_wActivityPostRetraction.csv",
                index=False)