import pandas as pd
import sys
import numpy as np

def get_stats(df_matched, key=['Record ID','MAGAID']):

        #grouped_size = df_matched.groupby(key).size()
        match_grouped_size = df_matched.groupby(key)['MatchMAGAID'].nunique()

        print("Total number of papers matched", df_matched['Record ID'].nunique())

        print("Authors matched",df_matched[~df_matched.MatchMAGAID.isna()].MAGAID.nunique())

        print("Average number of matches", match_grouped_size.mean())

        print("Minimum number of matches", match_grouped_size.min())

        print("Maximum number of matches", match_grouped_size.max())

PATH = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"

PATHOUT = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"

df_matched_rAff = pd.read_csv(PATH+"/RWMatched_retractionaff.csv")

df_matched_gender = pd.read_csv(PATH+"/RWMatched_wo_1DCollaborators.csv",
                                usecols=['Record ID','MAGPID', 'MAGAID','MatchMAGAID',
                                        'GenderizeGender', 'MAGFirstPubYear',
                                        'MAGFirstAffID','MatchMAGFirstAffID',
                                        'MatchMAGFirstYear','MAGFirstAffiliationRank',
                                        'MatchMAGFirstAffiliationRank']).\
                                        drop_duplicates().\
                                        rename(columns={'MatchMAGFirstYear':'MatchMAGFirstAffYear'})

df_matched_discipline = pd.read_csv(PATH+"/RWMatched_discipline.csv",
                        usecols=['Record ID',
                        'MAGPID', 'MAGAID', 'MAGrootFID', 'MAGrootFIDMaxPercent',
                        'MatchMAGAID', 'MatchMAGrootFID', 'MatchMAGrootFIDMaxPercent']).\
                                drop_duplicates()

# df_matched_papers = pd.read_csv(PATH+"/RWMatched_papers.csv",
#                                 usecols=['Record ID',
#                                 'MAGPID','MAGAID','MAGCumPapers','MAGCumPapersBins',
#                                 'MatchMAGAID','MatchMAGCumPapersYear','MatchMAGCumPapers',
#                                 'MatchMAGCumPapersBins']).\
#                                 drop_duplicates()

# df_matched_citations = pd.read_csv(PATH+"/RWMatched_citations.csv",
#                                 usecols=['Record ID',
#                                 'MAGPID','MAGAID','MAGCumCitations','MAGCumCitationsBins',
#                                 'MatchMAGAID','MatchMAGCumCitationsYear',
#                                 'MatchMAGCumCitations','MatchMAGCumCitationsBins']).\
#                                 drop_duplicates()

# df_matched_collaborators = pd.read_csv(PATH+"/RWMatched_collaborators.csv",
#                                 usecols=['Record ID',
#                                 'MAGPID','MAGAID',
#                                 'MAGCumCollaborators','MAGCumCollaboratorsBins',
#                                 'MatchMAGAID', 'MatchMAGCumCollaboratorsYear',
#                                 'MatchMAGCumCollaborators','MatchMAGCumCollaboratorsBins']).\
#                                 drop_duplicates()
                                
# Now let us merge them all
df_intersection = df_matched_rAff.\
                        merge(df_matched_discipline, 
                                        on=['Record ID', 'MAGPID', 'MAGAID', 'MatchMAGAID']).\
                        merge(df_matched_gender,
                                        on=['Record ID', 'MAGPID', 'MAGAID', 'MatchMAGAID'])
                        # merge(df_matched_papers, 
                        #                 on=['Record ID', 'MAGPID', 'MAGAID', 'MatchMAGAID']).\
                        # merge(df_matched_citations, 
                        #                 on=['Record ID', 'MAGPID', 'MAGAID', 'MatchMAGAID']).\
                        # merge(df_matched_collaborators, 
                        #                 on=['Record ID', 'MAGPID', 'MAGAID', 'MatchMAGAID']).\

get_stats(df_intersection)

df_intersection.to_csv(PATHOUT+"/RWMatched_intersection_woPapersCitationsCollaborators.csv", 
                        index=False)