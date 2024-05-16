import pandas as pd
import sys
import numpy as np

# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"


def get_stats(df_matched, key=['Record ID','MAGAID']):
    
    #grouped_size = df_matched.groupby(key).size()
    match_grouped_size = df_matched.groupby(key)['MatchMAGAID'].nunique()
    
    print("Total number of papers matched", df_matched['Record ID'].nunique())
    
    print("Authors matched",df_matched[~df_matched.MatchMAGAID.isna()].MAGAID.nunique())
    
    print("Average number of matches", match_grouped_size.mean())
    
    print("Minimum number of matches", match_grouped_size.min())

    print("Maximum number of matches", match_grouped_size.max())

def match_discipline(df_filtered_sample):
    
    # Let us now read the matched file
    df_matches = pd.read_csv(OUTDIR+"/RWMatched_wo_1DCollaborators.csv",
                            usecols=['MAGAID','MatchMAGAID','Record ID','MAGPID']).\
                                drop_duplicates()
    
    # Now let us augment retraction year and MAGrootFID
    df_matches = df_matches.merge(df_filtered_sample, on='MAGAID')
    
    # Now we will extract MAGrootFID and match it
    dfs = []
    
    # Go through each year (since files are separated by years)
    for year in df_filtered_sample.RetractionYear.unique():
        
        df_matches_i = df_matches[df_matches.RetractionYear.eq(year)]
        
        df_fields_i = pd.read_csv(INDIR_DERIVED+"/cumFields_max/AID_rootFID_"+str(int(year))+".csv",
                                    usecols=['AID','rootFID','maxPercent']).\
                                        rename(columns={'AID':'MatchMAGAID',
                                                        'rootFID':'MatchMAGrootFID',
                                                        'maxPercent':'MatchMAGrootFIDMaxPercent'})
        
        # Merging on MatchMAGAID to get the field for the match
        df_merged_i = df_matches_i.merge(df_fields_i, on='MatchMAGAID')
        
        # Checking if the field equals field of treatment
        df_matched_i = df_merged_i[df_merged_i.MatchMAGrootFID.eq(df_merged_i.MAGrootFID)]
        
        # Appending this sub-df to a list
        dfs.append(df_matched_i)
    
    df_matched = pd.concat(dfs)
    
    df_matched.to_csv(OUTDIR+"/RWMatched_discipline.csv", index=False)
    
    return df_matched

def main():
    
    # Let us first read filtered sample and disciplines
    df_filtered_sample = pd.read_csv(INDIR_PROCESSED+"/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections_Augmented.csv",
                                    usecols=['MAGAID','MAGrootFID','RetractionYear',
                                            'MAGrootFIDMaxPercent']).\
                                        drop_duplicates()
                                        
    # Now let us match discipline
    df_matched = match_discipline(df_filtered_sample)
    
    get_stats(df_matched)
    
    print("done.")
    
main()