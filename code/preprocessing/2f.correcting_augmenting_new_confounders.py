#!/usr/bin/env python

"""
Summary:
This Python program along with 2g will be used to
gather and augment the following new 
columns into the dataframe created for 
regression earlier 
i.e. RW_authors_w_confounders_filteredSample_postNHB.csv.

The new columns we are interested in are:
1. First affiliation rank
2. First year (this will vary a bit now because Bedoor's 
using MAG to compute the publication year, and some 
authors might now have attrition year before their 
first publication year. In any case, we need to correct for
this now)
3. Attrition Year (this is as computed by Bedoor)
4. Attrition Class (this will be based on the 
logic discussed with Bedoor)
5. Finally, we will correct the fields columnn such that 
each author has exactly 1 discipline.
"""

import pandas as pd
import os
from config_reader import read_config

def process_affiliation_ranks(df):
    """This function maps affiliation ranks to their 
    correct mapping using this mapping
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
    """
    
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
                '1001-350000':1500}
    
    def get_mapped_value(rank):
        if pd.isna(rank):
            return 1500 # This means rank was not found
        for key, value in rank_dict.items():
            low, high = key.split('-')
            if rank >= float(low) and rank <= float(high):
                return value
        return None  # Return None if no range matches (you can choose to handle this differently)

    # Apply the mapping function to the "FirstAffRank" column
    df['FirstAffRankCorrected'] = df['FirstAffRank'].apply(get_mapped_value)
    return df

def main():
    # reading all the relevant paths
    paths = read_config()
    # We will remove direct paths later.
    # Reading the older file on filtered sample
    df_filteredSample = pd.read_csv("/scratch/bka3/Retraction_data/processed/RW_authors_w_confounders_filteredSample_postNHB.csv")
    # Reading first affiliation rank
    df_firstAffRanks = pd.read_csv("/scratch/bka3/Retraction_data/MAG/derived/AID_Gender_FirstPubYear_FirstAffID_FirstAffRank.csv",
                                usecols=['AID','FirstAffID','FirstAffRank'])\
                                    .drop_duplicates()\
                                    .rename(columns={'AID':'MAGAID'})
    
    # Let us merge to get affiliation ranks of our retracted authors
    df_filteredSample2 = df_filteredSample.merge(df_firstAffRanks, on=['MAGAID'],
                                                how='left')
    
    # Processing affiliation ranks (correcting them to mid value)
    df_filteredSample2 = process_affiliation_ranks(df_filteredSample2)
    
    # reading attrition file
    df_attrition = pd.read_csv("/scratch/bka3/Retraction_data/MAG/derived/AIDs_YearOfAttrition_95percentile_allauthors.csv")\
                        .rename(columns={'AID':'MAGAID'})
                        
    df_filteredSample3 = df_filteredSample2.merge(df_attrition, 
                                                on='MAGAID', how='left')
    
    df_filteredSample3.to_csv("/scratch/bka3/Retraction_data/processed/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections.csv",
                            index=False)
    
if __name__ == "__main__":
    main()