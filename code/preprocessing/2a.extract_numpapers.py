#!/usr/bin/env python

"""
Summary:
This Python program focuses on extracting the number of 
papers for all the authors in MAG for all the years. 
We will have a csv with four columns:
- MAGAID
- Year
- numPapers
- CumPapers
"""

import pandas as pd
import os
import argparse
from config_reader import read_config
from data_utils import read_csv

def main():
    # reading all the relevant paths
    paths = read_config()
    INDIR_MAG_DERIVED_PATH = paths['INDIR_MAG_DERIVED_PATH']
    RETRACTION_NOTICES_POSTFILTERING_PATH = paths['RETRACTION_NOTICES_POSTFILTERING_PATH']
    
    # Reading the main PID_AID_AffID_year_rank file
    df_pid_aid_year = pd.read_csv(INDIR_MAG_DERIVED_PATH+"/PID_AID_AffID_year_rank.csv",
                                            usecols=['PID','AID','year'])\
                        .drop_duplicates()\
                        .rename(columns={'PID':'MAGPID',
                                        'AID':'MAGAID'})
    
    # Removing all the retraction notices
    notices = pd.read_csv(RETRACTION_NOTICES_POSTFILTERING_PATH)\
                    .rename(columns={'PID':'MAGPID'})['MAGPID'].unique()
                    
    df_pid_aid_year = df_pid_aid_year[~df_pid_aid_year['MAGPID'].isin(notices)]
    
    # Grouping by year and MAGPID and computing unique papers for each year
    df_aid_year_numPapers = df_pid_aid_year.groupby(["MAGAID","year"]).nunique()\
                                .reset_index().\
                                rename(columns={"MAGPID":"numPapers"})
            
    # Finally computing the cumulative for each year starting from the earliest year
    # In other words, we are first sorting by year
    # Then we are grouping by author
    # And then transforming using cumsum (which will compute cumulative)
    df_aid_year_numPapers['cumPapers'] = df_aid_year_numPapers.\
                                            sort_values(by="year").\
                                            groupby(["MAGAID"])['numPapers'].transform('cumsum')
            
    # Save the relevant data
    df_aid_year_numPapers.to_csv(os.path.join(INDIR_MAG_DERIVED_PATH, "MAGAID_PubYear_numPapers_cumPapers_correctedPostNHB.csv"), 
                        index=False)

if __name__ == "__main__":
    main()