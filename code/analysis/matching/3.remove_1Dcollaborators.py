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

def extract_rw_collaborators(df_authors):
    
    """
    In this function, we will extract all
    the collaborators for RW authors. 
    
    This is a NO-LIST as such we cannot
    match rw_authors to their collaborators.
    """
    
    # Extracting only author IDs
    df_authors = df_authors[['MAGAID','RetractionYear']].drop_duplicates()
    
    # Let us load the aid, pids to extract collaborators
    df_papers = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
                                    usecols=['PID','AID','year'])\
                                        .rename(columns={"AID":"CollaboratorMAGAID",
                                                "PID":"CollaboratorMAGPID",
                                                "year":"CollaboratorMAGPIDYear"}).\
                                                    drop_duplicates()
    
    # Removing papers without year
    #df_papers = df_papers[~df_papers.CollaboratorMAGPIDYear.isna()]
    
    # Extracting rw authors' papers to get MAGPID and Year
    df_rw_papers = df_authors.merge(df_papers, left_on='MAGAID',
                                        right_on='CollaboratorMAGAID',
                                        how='left').\
                                        drop(columns=['CollaboratorMAGAID'])
    
    # Now let us extract collaborators
    df_rw_collaborators = df_rw_papers.merge(df_papers, 
                                        on=['CollaboratorMAGPID'],
                                        how='left')

    # Now let us save this
    df_rw_collaborators.to_csv(OUTDIR+"/RW_collaborators_byRetraction.csv",index=False)
    
    return df_rw_collaborators

def main():
    
    df_authors = pd.read_csv(INDIR_PROCESSED+"/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections_Augmented.csv",
                        usecols=['MAGAID','Record ID','MAGPID','RetractionYear'])
    
    # Reading first degree collaborators
    df_collaborators = extract_rw_collaborators(df_authors)
    
    # Creating a list of collaborators for filtering
    lst_collaborators = df_collaborators['CollaboratorMAGAID'].tolist() + \
                df_collaborators['MAGAID'].tolist()
                
    # Reading the matched file
    df_matched = pd.read_csv(OUTDIR+"RWMatched_gender_firstPubYear_firstAff.csv")
                
    # Let us now remove all collaborators from the dataframe
    df_matched_filtered = df_matched[~df_matched.MatchMAGAID.isin(lst_collaborators)]
    
    get_stats(df_matched_filtered)
    
    df_matched_filtered.to_csv(OUTDIR+"/RWMatched_wo_1DCollaborators.csv", index=False)
    
main()