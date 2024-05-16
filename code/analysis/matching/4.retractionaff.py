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

def match_ryear_top100firstaff(df_authors_top100aff):
        
        # First let us read matches
        
        df_matches = pd.read_csv(OUTDIR+"/RWMatched_wo_1DCollaborators.csv",
                                usecols=['MAGAID','MatchMAGAID',
                                        'Record ID', 'MAGPID']).drop_duplicates()
        
        # Let us augment these matches with relevant field of affiliation
        df_matches = df_matches.merge(df_authors_top100aff, on='MAGAID')
        
        # Loading affiliations        
        # Only keeping affiliations where rank is between 101-1000 and retraction aff year is given
        # We are also only keeping the first affiliations of authors
        df_affiliations = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
                                        usecols=['AID','AffID','year','rank'])\
                                                .rename(columns={"AID":"MatchMAGAID",
                                                        "AffID":"MatchMAGRetractionYearAffID",
                                                        "year":"MatchMAGRetractionYearAffYear",
                                                        "rank":"MatchMAGRetractionYearAffRank"}).\
                                                        drop_duplicates()
                                                        
        
        # Only keeping affiliations where rank is not NaN or 1-100,
        # and retraction aff year is given
        # We are also only keeping the first affiliations of authors
        top_100 = [str(rank) for rank in range(101)]
        df_affiliations = df_affiliations[~df_affiliations.MatchMAGRetractionYearAffID.isna() &
                                        ~df_affiliations.MatchMAGRetractionYearAffYear.isna() & 
                                        df_affiliations.MatchMAGRetractionYearAffRank.isin(top_100)]
                                        
                                        
        # Let us now merge affiliations to matches
        df_merged = df_matches.merge(df_affiliations, on='MatchMAGAID',
                                        how='left').drop_duplicates().\
                                sort_values(by='MatchMAGRetractionYearAffYear')
                                
        # Let us remove all affiliations after the year of retraction
        df_merged = df_merged[df_merged.MatchMAGRetractionYearAffYear.\
                                le(df_merged.RetractionYear)]
        
        #Now let us get the max affiliation year for each matchmagaid
        df_merged['MatchMAGMaxRetractionYear'] = df_merged\
                .groupby(['Record ID','MAGAID','MAGPID','MatchMAGAID'])\
                        ['MatchMAGRetractionYearAffYear'].transform('max')
        
        # Finally let us remove all others and keep the max (this is interpolated retration year and affiliation for match)
        df_merged = df_merged[df_merged.MatchMAGMaxRetractionYear.\
                eq(df_merged.MatchMAGRetractionYearAffYear)]
        
        df_matched = df_merged[(df_merged.MAGRetractionYearAffRank.astype(int)-\
                                df_merged.MatchMAGRetractionYearAffRank.astype(int)).\
                                abs().le(2)]
        
        print("Stats for 1-100:")            
        get_stats(df_matched)
        return df_matched                        

def match_ryear_101to1000firstaff(df_authors_101to1000):
        # First let us read matches
        
        df_matches = pd.read_csv(OUTDIR+"/RWMatched_wo_1DCollaborators.csv",
                                usecols=['MAGAID','MatchMAGAID',
                                        'Record ID', 'MAGPID']).drop_duplicates()
        
        # Let us augment these matches with relevant field of affiliation
        df_matches = df_matches.merge(df_authors_101to1000, on='MAGAID')
        
        # Loading affiliations
        # Let us load the affiliations dataframe to get first found affiliation
        df_affiliations = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
                                        usecols=['AID','AffID','year','rank'])\
                                                .rename(columns={"AID":"MatchMAGAID",
                                                        "AffID":"MatchMAGRetractionYearAffID",
                                                        "year":"MatchMAGRetractionYearAffYear",
                                                        "rank":"MatchMAGRetractionYearAffRank"}).\
                                                        drop_duplicates()
                                                        
        
        # Only keeping affiliations where rank is not NaN or 1-100,
        # and retraction aff year is given
        # We are also only keeping the first affiliations of authors
        top_100 = [str(rank) for rank in range(101)]
        df_affiliations = df_affiliations[~df_affiliations.MatchMAGRetractionYearAffRank.isna() &
                                        ~df_affiliations.MatchMAGRetractionYearAffID.isna() &
                                        ~df_affiliations.MatchMAGRetractionYearAffYear.isna() & 
                                        ~df_affiliations.MatchMAGRetractionYearAffRank.isin(top_100)]
                                        
        # Let us now merge affiliations to matches
        df_merged = df_matches.merge(df_affiliations, on='MatchMAGAID',
                                        how='left').drop_duplicates().\
                                        sort_values(by='MatchMAGRetractionYearAffYear')
                                        
        # Let us remove all affiliations after the year of retraction
        df_merged = df_merged[df_merged.MatchMAGRetractionYearAffYear.\
                                le(df_merged.RetractionYear)]
        
        #Now let us get the max affiliation year for each matchmagaid
        df_merged['MatchMAGMaxRetractionYear'] = df_merged\
                .groupby(['Record ID','MAGAID','MAGPID','MatchMAGAID'])\
                        ['MatchMAGRetractionYearAffYear'].transform('max')
        
        # Finally let us remove all others and keep the max (this is interpolated retration year and affiliation for match)
        df_merged = df_merged[df_merged.MatchMAGMaxRetractionYear.\
                eq(df_merged.MatchMAGRetractionYearAffYear)]
        
        # Now we need to match the exact affiliation
        df_matched = df_merged[df_merged['MAGRetractionYearAffRank'].\
                                eq(df_merged['MatchMAGRetractionYearAffRank'])].\
                                        drop_duplicates()
        print("Stats for 101-1000:")            
        get_stats(df_matched)
        
        return df_matched

def match_ryear_gt1000firstaff(df_authors_gt1000):
        
        # First let us read matches
        
        df_matches = pd.read_csv(OUTDIR+"/RWMatched_wo_1DCollaborators.csv",
                                usecols=['MAGAID','MatchMAGAID',
                                        'Record ID', 'MAGPID']).drop_duplicates()
        
        # Let us augment these matches with relevant field of affiliation
        df_matches = df_matches.merge(df_authors_gt1000, on='MAGAID')
        
        # Loading affiliations
        # Let us load the affiliations dataframe to get first found affiliation
        df_affiliations = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
                                        usecols=['AID','AffID','year','rank'])\
                                                .rename(columns={"AID":"MatchMAGAID",
                                                        "AffID":"MatchMAGRetractionYearAffID",
                                                        "year":"MatchMAGRetractionYearAffYear",
                                                        "rank":"MatchMAGRetractionYearAffRank"}).\
                                                        drop_duplicates()
                                                        
        
        # Only keeping affiliations where rank is NaN and retraction aff year is given
        # We are also only keeping the first affiliations of authors
        
        df_affiliations = df_affiliations[df_affiliations.MatchMAGRetractionYearAffRank.isna() &
                                        ~df_affiliations.MatchMAGRetractionYearAffID.isna() &
                                        ~df_affiliations.MatchMAGRetractionYearAffYear.isna()]

        # Let us now merge affiliations to matches
        df_merged = df_matches.merge(df_affiliations, on='MatchMAGAID',
                                        how='left').drop_duplicates().\
                                        sort_values(by='MatchMAGRetractionYearAffYear')
                                        
        # Let us remove all affiliations after the year of retraction
        df_merged = df_merged[df_merged.MatchMAGRetractionYearAffYear.\
                                le(df_merged.RetractionYear)]
        
        #Now let us get the max affiliation year for each matchmagaid
        df_merged['MatchMAGMaxRetractionYear'] = df_merged\
                .groupby(['Record ID','MAGAID','MAGPID','MatchMAGAID'])\
                        ['MatchMAGRetractionYearAffYear'].transform('max')
        
        # Finally let us remove all others and keep the max (this is interpolated retration year and affiliation for match)
        df_merged = df_merged[df_merged.MatchMAGMaxRetractionYear.\
                eq(df_merged.MatchMAGRetractionYearAffYear)]
        
        # Now we need to match the exact affiliation
        df_matched = df_merged[df_merged['MAGRetractionYearAffID'].\
                                eq(df_merged['MatchMAGRetractionYearAffID'])].\
                                        drop_duplicates()
        print("Stats for >1000:")            
        get_stats(df_matched)
        
        return df_matched
        

def main():
        # Reading the filtered sample (only relevant columns)
        df_authors = pd.read_csv(INDIR_PROCESSED+"/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections_Augmented.csv",
                                usecols=['MAGAID',
                                        'RetractionYear',
                                        'MAGRetractionYearAffID',
                                        'MAGRetractionYearAffRank',
                                        'MAGRetractionYearAffYear']).\
                                drop_duplicates()
        
        # First let us extract authors with affiliation rank in top 100
        top_100 = [str(rank) for rank in range(101)]
        
        # Running matching for authors in top 100
        df_authors_top100aff = df_authors[df_authors.MAGRetractionYearAffRank.isin(top_100)]
                
        # Now let us extract authors with affiliation rank between 100 and 1000
        df_authors_101to1000 = df_authors[~df_authors.MAGRetractionYearAffRank.isin(top_100) & 
                                                ~df_authors.MAGRetractionYearAffRank.eq('1001-')]

        # Now let us extract authors with affiliation rank above 1000
        df_authors_gt1000 = df_authors[df_authors.MAGRetractionYearAffRank.eq('1001-')]
        
        # Let us do a sensibility check here to check all authors are covered.
        # Note: There may be overlap of authors between three categories due to mult. affiliations
        assert(len(set(list(df_authors_top100aff.MAGAID.unique()) + \
                        list(df_authors_101to1000.MAGAID.unique()) + \
                        list(df_authors_gt1000.MAGAID.unique()))) == df_authors.MAGAID.nunique())
        
        # Now we shall deal with each of these three separately
        
        df1 = match_ryear_top100firstaff(df_authors_top100aff)
        
        df2 = match_ryear_101to1000firstaff(df_authors_101to1000)
        
        df3 = match_ryear_gt1000firstaff(df_authors_gt1000)
        
        # Once we have matched all three, we will simply concatenate them
        df_matched = pd.concat([df1,df2,df3]).drop_duplicates()
        
        get_stats(df_matched)
        
        df_matched.to_csv(OUTDIR+"/RWMatched_retractionaff.csv",index=False)

        print("done")

main()