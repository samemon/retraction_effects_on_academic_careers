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
    
    grouped_size = df_matched.groupby(key).size()
    match_grouped_size = df_matched.groupby(key)['MatchMAGAID'].nunique()
    
    print("Total number of papers matched", df_matched['Record ID'].nunique())
    
    print("Authors matched",df_matched[~df_matched.MatchMAGAID.isna()].MAGAID.nunique())
    
    print("Average number of matches", match_grouped_size.mean())
    
    print("Minimum number of matches", match_grouped_size.min())

    print("Maximum number of matches", match_grouped_size.max())

def match_papers_cites_collabs(df_regression_sample):
    
    # First we need to load papers, citations, and collaborators for retracted authors
    
    # Loading papers file
    df_papers = pd.read_csv(INDIR_DERIVED+"/MAGAID_PubYear_numPapers_cumPapers_correctedPostNHB.csv",
                                usecols=['MAGAID','year','cumPapers']).\
                                    rename(columns={'MAGAID':'MatchMAGAID',
                                                    'year':'MatchMAGCumPapersYearAtRetraction',
                                                    'cumPapers':'MatchMAGCumPapersAtRetraction'})
    
    print("Data type for AID:", df_papers.MatchMAGAID.dtype)
    print("Data type for Year:", df_papers.MatchMAGCumPapersYearAtRetraction.dtype)
    
    # Loading the citations first
    df_citations = pd.read_csv(INDIR_DERIVED+"/AID_year_newCitations_cumCitations_corrected.csv",
                                usecols=['MAGAID','PID_PubYear','cumCitations']).\
                                    rename(columns={'MAGAID':'MatchMAGAID',
                                                    'PID_PubYear':'MatchMAGCumCitationsYearAtRetraction',
                                                    'cumCitations':'MatchMAGCumCitationsAtRetraction'})
                                    
    print("Data type for AID:", df_citations.MatchMAGAID.dtype)
    print("Data type for Year:", df_citations.MatchMAGCumCitationsYearAtRetraction.dtype)
    
    df_collaborators = pd.read_csv(INDIR_DERIVED+"/AID_year_numCollaborators.csv",
                                usecols=['AID','PubYear','cumCollaborators']).\
                                    rename(columns={'AID':'MatchMAGAID',
                                                    'PubYear':'MatchMAGCumCollaboratorsYearAtRetraction',
                                                    'cumCollaborators':'MatchMAGCumCollaboratorsAtRetraction'})
                                    
    print("Data type for AID:", df_collaborators.MatchMAGAID.dtype)
    print("Data type for Year:", df_collaborators.MatchMAGCumCollaboratorsYearAtRetraction.dtype)
    
    # Now let us read matches
    df_matches = pd.read_csv(OUTDIR+"/RWMatched_intersection_woPapersCitationsCollaborators.csv",
                            usecols=['MAGAID','MatchMAGAID','Record ID','MAGPID', 
                                    'RetractionYear']).\
                                drop_duplicates()
                                
    # Let us standardize columns
    # df_matches['MAGAID'] = df_matches['MAGAID'].astype(np.int64)
    # df_matches['MatchMAGAID'] = df_matches['MAGAID'].astype(np.int64)

    print("Stats for matches")
    get_stats(df_matches)
    
    # Let us first add columns for treatment
    df_matches = df_matches.merge(df_regression_sample, on='MAGAID',
                                    how='left')
    
    #Now we shall merge this with papers, citations, collaborators
    
    # PROCESSING PAPERS FIRST
    rel_cols = ['MAGAID','MatchMAGAID','Record ID','MAGPID','RetractionYear',
                'MAGCumPapersAtRetraction', 'MAGCumPapersYearAtRetraction',]
    
    df_merged_papers = df_matches[rel_cols].merge(df_papers, on='MatchMAGAID', how='left')
    
    df_merged_papers = df_merged_papers[df_merged_papers.\
                                MatchMAGCumPapersYearAtRetraction.le(df_merged_papers.RetractionYear)].\
                                    sort_values(by='MatchMAGCumPapersYearAtRetraction').\
                                        drop_duplicates(subset=['Record ID','MAGPID','MAGAID',
                                                        'MatchMAGAID','RetractionYear'], keep='last')
                                        
                                        
    # df_merged_papers.to_csv(OUTDIR+"/RWMatched_intersection_w_PapersAtRetraction.csv", 
    #                                 index=False)
    
    # sys.exit()
                                        
    #assert(df_merged_papers.MAGAID.nunique() == df_matches.MAGAID.nunique())
    #assert(df_merged_papers.MatchMAGAID.nunique() == df_matches.MatchMAGAID.nunique())
    
    # PROCESSING CITATIONS
    rel_cols = ['MAGAID','MatchMAGAID','Record ID','MAGPID','RetractionYear',
                'MAGCumCitationsAtRetraction', 'MAGCumCitationsYearAtRetraction',]
    
    df_merged_citations = df_matches[rel_cols].merge(df_citations, on='MatchMAGAID', how='left')
    
    df_merged_citations = df_merged_citations[df_merged_citations.\
                                MatchMAGCumCitationsYearAtRetraction.le(df_merged_citations.RetractionYear)].\
                                    sort_values(by='MatchMAGCumCitationsYearAtRetraction').\
                                        drop_duplicates(subset=['Record ID','MAGPID','MAGAID',
                                                        'MatchMAGAID','RetractionYear'], keep='last')
    
    #assert(df_merged_citations.MAGAID.nunique() == df_matches.MAGAID.nunique())
    #assert(df_merged_citations.MatchMAGAID.nunique() == df_matches.MatchMAGAID.nunique())
    
    # PROCESSING COLLABORATORS
    rel_cols = ['MAGAID','MatchMAGAID','Record ID','MAGPID','RetractionYear',
                'MAGCumCollaboratorsAtRetraction', 'MAGCumCollaboratorsYearAtRetraction',]
    
    df_merged_collaborators = df_matches[rel_cols].merge(df_collaborators, on='MatchMAGAID', how='left')
    
    df_merged_collaborators = df_merged_collaborators[df_merged_collaborators.\
                                MatchMAGCumCollaboratorsYearAtRetraction.le(df_merged_collaborators.RetractionYear)].\
                                    sort_values(by='MatchMAGCumCollaboratorsYearAtRetraction').\
                                        drop_duplicates(subset=['Record ID','MAGPID','MAGAID',
                                                        'MatchMAGAID','RetractionYear'], keep='last')
                                        
    #assert(df_merged_collaborators.MAGAID.nunique() == df_matches.MAGAID.nunique())
    #assert(df_merged_collaborators.MatchMAGAID.nunique() == df_matches.MatchMAGAID.nunique())
    
    # Now let us merge all three dataframes
    default_cols = ['MAGAID','MatchMAGAID','Record ID','MAGPID','RetractionYear']
    df_merged_all = df_matches[default_cols].merge(df_merged_papers, on=default_cols, how='left')\
                                            .merge(df_merged_citations, on=default_cols, how='left')\
                                            .merge(df_merged_collaborators, on=default_cols, how='left')

    # Now I need to fill NaNs in papers, citations, and collaborators with 0
    print("Number of NaNs for papers (Matches)", df_merged_all[df_merged_all.\
                                MatchMAGCumPapersAtRetraction.isna()].MatchMAGAID.nunique())
    
    print("Number of NaNs for papers (MAGAIDS)", df_merged_all[df_merged_all.\
                                MatchMAGCumPapersAtRetraction.isna()].MAGAID.nunique())
    
    print("Number of NaNs for citations (Matches)", df_merged_all[df_merged_all.\
                                MatchMAGCumCitationsAtRetraction.isna()].MatchMAGAID.nunique())
    
    print("Number of NaNs for citations (MAGAIDS)", df_merged_all[df_merged_all.\
                                MatchMAGCumCitationsAtRetraction.isna()].MAGAID.nunique())
    
    print("Number of NaNs for collaborators (Matches)", df_merged_all[df_merged_all.\
                                MatchMAGCumCollaboratorsAtRetraction.isna()].MatchMAGAID.nunique())
    
    print("Number of NaNs for collaborators (MAGAIDS)", df_merged_all[df_merged_all.\
                                MatchMAGCumCollaboratorsAtRetraction.isna()].MAGAID.nunique())
    
    df_merged_all['MatchMAGCumPapersAtRetraction'] = df_merged_all['MatchMAGCumPapersAtRetraction'].fillna(0)
    df_merged_all['MatchMAGCumCitationsAtRetraction'] = df_merged_all['MatchMAGCumCitationsAtRetraction'].fillna(0)
    df_merged_all['MatchMAGCumCollaboratorsAtRetraction'] = df_merged_all['MatchMAGCumCollaboratorsAtRetraction'].fillna(0)
    
    print(df_merged_all.shape, df_matches.shape)
    
    df_merged_all.to_csv(OUTDIR+"/RWMatched_intersection_w_PapersCitationsCollaboratorsAtRetraction.csv", 
                                    index=False)
    
    return df_merged_all
        

def main():
    
    # Let us first read filtered sample to compute bins
    df_regression_sample = pd.read_csv(INDIR_PROCESSED+"/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections_Augmented.csv",
                                    usecols=['MAGAID', 'MAGCumPapersAtRetraction', 
                                            'MAGCumPapersYearAtRetraction',
                                            'MAGCumCitationsAtRetraction',
                                            'MAGCumCitationsYearAtRetraction',
                                            'MAGCumCollaboratorsAtRetraction',
                                            'MAGCumCollaboratorsYearAtRetraction',
                                            'nRetracted', 'AttritedClass']).\
                                        drop_duplicates()
    
    df_regression_sample = df_regression_sample[df_regression_sample['nRetracted'].eq(1) &
                                                df_regression_sample['AttritedClass'].eq(0)]
    
    # Now we shall match papers
    df_matched = match_papers_cites_collabs(df_regression_sample)
    
    print("Stats for augmented matches")
    get_stats(df_matched)
    
    print("done.")

main()