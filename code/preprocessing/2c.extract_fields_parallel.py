"""
This program will extract fields 
for each retracted author.
"""

import pandas as pd
import sys

def extract_fields_new_parallel(year):
    
    indir = "/scratch/sm9654/retraction_openalex/data/processed/"
    indir2 = "/scratch/sm9654/data/MAG_2021/derived/cumFields_max/"
    
    df_authors = pd.read_csv(indir+"RWMAG_authors_SingleAndRepeatedOffendersSameYear.csv")
        
    df_relevant_authors = df_authors[df_authors.RetractionYear.eq(year)]
        
    df_fields = pd.read_csv(indir2+"AID_rootFID_"+\
            str(year)+".csv", usecols=['AID','rootFID','maxPercent'])\
        .rename(columns={'AID':'MAGAID',
                        'rootFID':'MAGrootFID',
                        'maxPercent': 'MAGrootFIDMaxPercent'})
    
    df_merged = df_relevant_authors.merge(df_fields, 
                                        on="MAGAID", how="left")
    
    df_merged.to_csv(indir+"/RW_authors_w_fields/"+str(year)+".csv",index=False)


if __name__ == "__main__":
    
    """
    Let us first take in one argument: (i) target_year
    """
    
    argv = sys.argv[1:]
    if(len(argv) != 1):
        # If less than or greater than 1 arguments, give error
        print("Usage: python3 2d.extract_fields_parallel.py <target_year>")
        sys.exit()
    else:
        target_year = int(float(argv[0]))
        extract_fields_new_parallel(target_year)
