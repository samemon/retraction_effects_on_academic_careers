import pandas as pd
import sys
import numpy as np

def get_stats(df_matched, key=['Record ID','MAGAID']):
    
    grouped_size = df_matched.groupby(key).size()
    match_grouped_size = df_matched.groupby(key)['MatchMAGAID'].nunique()
    
    print("Total number of papers matched", df_matched['Record ID'].nunique())
    
    print("Authors matched",df_matched[~df_matched.MatchMAGAID.isna()].MAGAID.nunique())
    
    print("Average number of matches", match_grouped_size.mean())
    
    print("Minimum number of matches", match_grouped_size.min())

    print("Maximum number of matches", match_grouped_size.max())

# directory where we shall read and save all the new files
PATH = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"
# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

        
# We have created three files because of how we deal with affiliation rank
# In this program, we shall merge those 3 files into one.
df_authors = pd.read_csv(INDIR_PROCESSED+"/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections_Augmented.csv",
                        usecols=['MAGAID','Record ID','MAGPID'])

df1 = pd.read_csv(PATH+"RWMatched_gender_firstPubYear_top100firstAff.csv")
df2 = pd.read_csv(PATH+"RWMatched_gender_firstPubYear_101to1000firstAff.csv")
df3 = pd.read_csv(PATH+"RWMatched_gender_firstPubYear_gt1000firstAff.csv")

df3['MAGFirstAffiliationRank'] = '1001-'
df3['MatchMAGFirstAffiliationRank'] = '1001-'

df4 = pd.concat([df1,df2,df3])

print("Concatenated file:",df4.shape)

df4 = df4.drop_duplicates()

print("File after dropping duplicates:",df4.shape)

df_matched = df4[~df4.MatchMAGAID.isna()]

df_matched = df_matched.merge(df_authors, on='MAGAID')

print("File after dropping those without matches:",df_matched.shape)

# Let us check how many were matched.
# Those matched will have MatchMAGAID as non null
get_stats(df_matched)

df_matched.to_csv(PATH+"/RWMatched_gender_firstPubYear_firstAff.csv",
                index=False)