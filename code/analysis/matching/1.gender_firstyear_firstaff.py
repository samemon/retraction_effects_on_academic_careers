"""
This program shall be used to 
match gender, first year and first 
affiliation of authors in RW and in MAG. 

For first affiliation, we shall match 
the three categories differently i.e. 
1) For affiliation between 1-100, we shall 
match them with +-2 difference
2) For affiliation rank between 101-1000, we 
shall match them with their given categories
3) For affiliation rank >1000, we shall match 
them exactly as per the affiliation id.
"""


import pandas as pd
import sys
import numpy as np

# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"

def read_gender_firstyear_for_matches():
        # let us load the first pub year file and rename
        df_firstyear = pd.read_csv(INDIR_DERIVED+"/AID_PID_firstPubYear.csv",
                                        usecols=['AID','year'])\
                                                .rename(columns={'AID':'MatchMAGAID',
                                                        'year':'MatchMAGFirstPubYear'})\
                                                .drop_duplicates()
        
        print("loaded first pubyear file")

        # Let us first load the MAG gender file
        df_gender = pd.read_csv(INDIR_DERIVED+"/AID_firstname_gender.csv",
                                        usecols=['AID','gender','confidence'])\
                                        .rename(columns={'AID':'MatchMAGAID',
                                                        'gender':'MatchGenderizeGender',
                                                        'confidence':'MatchGenderizeConfidence'})
        
        # Removing gender values with low confidence
        df_gender = df_gender[df_gender.MatchGenderizeConfidence.gt(0.5)]
        
        print("loaded gender file and filtered < 0.5")
        
        # Merging two dataframes
        df_gender_firstyear = df_gender.\
                merge(df_firstyear, on='MatchMAGAID')
        
        print("merged gender to first pub year")
        
        return df_gender_firstyear

def match_gender_firstyear_top100firstaff(df_authors_top100aff):
        
        # Loading affiliations
        # Let us load the affiliations dataframe to get first found affiliation
        df_affiliations = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
                                        usecols=['AID','AffID','year','rank'])\
                                                .rename(columns={"AID":"MatchMAGAID",
                                                        "AffID":"MatchMAGFirstAffID",
                                                        "year":"MatchMAGFirstYear",
                                                        "rank":"MatchMAGFirstAffiliationRank"}).\
                                                        drop_duplicates()
                                                        
        
        # Only keeping affiliations where rank is between 101-1000 and first aff year is given
        # We are also only keeping the first affiliations of authors
        top_100 = [str(rank) for rank in range(101)]
        df_affiliations = df_affiliations[df_affiliations.MatchMAGFirstAffiliationRank.isin(top_100) &
                                        ~df_affiliations.MatchMAGFirstYear.isna()].\
                                                sort_values(by='MatchMAGFirstYear')
                                                
        # One person can have multiple affiliations in the first year, so we do something extra
        df_firstaffyear = df_affiliations.drop_duplicates(subset='MatchMAGAID')\
                                [['MatchMAGAID','MatchMAGFirstYear']].\
                                rename(columns={'MatchMAGFirstYear':'MatchFirstAffYear'})
        
        df_affiliations = df_affiliations.merge(df_firstaffyear, on='MatchMAGAID')
        
        df_affiliations = df_affiliations[df_affiliations.MatchMAGFirstYear.\
                                        eq(df_affiliations.MatchFirstAffYear)].\
                                drop(columns=['MatchFirstAffYear'])
        
        # Reading gender and first pubyear
        df_gender_firstyear = read_gender_firstyear_for_matches()
        
        # Merging gender first year and first affiliation          
        df_gender_firstyear_firstaffiliation = df_gender_firstyear.\
                                                merge(df_affiliations, on='MatchMAGAID')
                                                
        # Extracting relevant columns for df_authors (dropping duplicates due to field)
        df_authors_relevant = df_authors_top100aff[['MAGAID',
                                                'GenderizeGender',
                                                'MAGFirstPubYear',
                                                'MAGFirstAffID',
                                                'MAGFirstAffiliationRank']]\
                                                .drop_duplicates()
        
        # Matching based on gender and first year
        df_matched = df_authors_relevant\
                        .merge(df_gender_firstyear_firstaffiliation,
                        left_on=['GenderizeGender',
                                'MAGFirstPubYear'],
                        right_on=['MatchGenderizeGender',
                                'MatchMAGFirstPubYear'],
                        how='left')\
                        .drop_duplicates()
                        
        # We have matched based on gender and first pub year
        # Now let us match based on ranking +- 2
        
        df_matched = df_matched[(df_matched.MAGFirstAffiliationRank.astype(int)-\
                                df_matched.MatchMAGFirstAffiliationRank.astype(int)).\
                                abs().le(2)]
        
        # Let us check how many were matched.
        # Those matched will have MatchMAGAID as non null
        print("# Matched 1-100", 
                df_matched[~df_matched.MatchMAGAID.isna()]['MAGAID'].nunique())
        
        print("# Not Matched 1-100", 
                df_matched[df_matched.MatchMAGAID.isna()]['MAGAID'].nunique())
        
        df_matched.to_csv(OUTDIR+"/RWMatched_gender_firstPubYear_top100firstAff.csv",
                        index=False)

def match_gender_firstyear_101to1000firstaff(df_authors_101to1000):
        
        # Loading affiliations
        # Let us load the affiliations dataframe to get first found affiliation
        df_affiliations = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
                                        usecols=['AID','AffID','year','rank'])\
                                                .rename(columns={"AID":"MatchMAGAID",
                                                        "AffID":"MatchMAGFirstAffID",
                                                        "year":"MatchMAGFirstYear",
                                                        "rank":"MatchMAGFirstAffiliationRank"}).\
                                                        drop_duplicates()
                                                        
        
        # Only keeping affiliations where rank is between 101-1000 and first aff year is given
        # We are also only keeping the first affiliations of authors
        top_100 = [str(rank) for rank in range(101)]
        df_affiliations = df_affiliations[~df_affiliations.MatchMAGFirstAffiliationRank.isna() &
                                        ~df_affiliations.MatchMAGFirstAffiliationRank.isin(top_100) &
                                        ~df_affiliations.MatchMAGFirstYear.isna()].\
                                                sort_values(by='MatchMAGFirstYear')
                                                
        # One person can have multiple affiliations in the first year, so we do something extra
        df_firstaffyear = df_affiliations.drop_duplicates(subset='MatchMAGAID')\
                                [['MatchMAGAID','MatchMAGFirstYear']].\
                                rename(columns={'MatchMAGFirstYear':'MatchFirstAffYear'})
        
        df_affiliations = df_affiliations.merge(df_firstaffyear, on='MatchMAGAID')
        
        df_affiliations = df_affiliations[df_affiliations.MatchMAGFirstYear.\
                                        eq(df_affiliations.MatchFirstAffYear)].\
                                drop(columns=['MatchFirstAffYear'])
        
        # Reading gender and first pubyear
        df_gender_firstyear = read_gender_firstyear_for_matches()
        
        # Merging gender first year and first affiliation          
        df_gender_firstyear_firstaffiliation = df_gender_firstyear.\
                                                merge(df_affiliations, on='MatchMAGAID')
                                                
        # Extracting relevant columns for df_authors (dropping duplicates due to field)
        df_authors_relevant = df_authors_101to1000[['MAGAID',
                                                'GenderizeGender',
                                                'MAGFirstPubYear',
                                                'MAGFirstAffID',
                                                'MAGFirstAffiliationRank']]\
                                                .drop_duplicates()
        
        # Matching based on gender and first year and first affiliation
        df_matched = df_authors_relevant\
                        .merge(df_gender_firstyear_firstaffiliation,
                        left_on=['GenderizeGender',
                                'MAGFirstPubYear',
                                'MAGFirstAffiliationRank'],
                        right_on=['MatchGenderizeGender',
                                'MatchMAGFirstPubYear',
                                'MatchMAGFirstAffiliationRank'],
                        how='left')\
                        .drop_duplicates()
        
        # Let us check how many were matched.
        # Those matched will have MatchMAGAID as non null
        print("# Matched 101-1000", 
                df_matched[~df_matched.MatchMAGAID.isna()]['MAGAID'].nunique())
        
        print("# Not Matched 101-1000", 
                df_matched[df_matched.MatchMAGAID.isna()]['MAGAID'].nunique())
        
        df_matched.to_csv(OUTDIR+"/RWMatched_gender_firstPubYear_101to1000firstAff.csv",
                        index=False)
        

def match_gender_firstyear_gt1000firstaff(df_authors_gt1000):
        
        # Loading affiliations
        # Let us load the affiliations dataframe to get first found affiliation
        df_affiliations = pd.read_csv(INDIR_DERIVED+"/PID_AID_AffID_year_rank.csv",
                                        usecols=['AID','AffID','year','rank'])\
                                                .rename(columns={"AID":"MatchMAGAID",
                                                        "AffID":"MatchMAGFirstAffID",
                                                        "year":"MatchMAGFirstYear",
                                                        "rank":"MatchMAGFirstAffiliationRank"}).\
                                                        drop_duplicates()
                                                        
        
        # Only keeping affiliations where rank is NaN and first aff year is given
        # We are also only keeping the first affiliations of authors
        
        df_affiliations = df_affiliations[df_affiliations.MatchMAGFirstAffiliationRank.isna() &
                                        ~df_affiliations.MatchMAGFirstYear.isna()].\
                                                sort_values(by='MatchMAGFirstYear')
                                                
        # One person can have multiple affiliations in the first year, so we do something extra
        df_firstaffyear = df_affiliations.drop_duplicates(subset='MatchMAGAID')\
                                [['MatchMAGAID','MatchMAGFirstYear']].\
                                rename(columns={'MatchMAGFirstYear':'MatchFirstAffYear'})
        
        df_affiliations = df_affiliations.merge(df_firstaffyear, on='MatchMAGAID')
        
        df_affiliations = df_affiliations[df_affiliations.MatchMAGFirstYear.\
                                        eq(df_affiliations.MatchFirstAffYear)].\
                                drop(columns=['MatchFirstAffYear'])
        
        # Reading gender and first pubyear
        df_gender_firstyear = read_gender_firstyear_for_matches()
        
        # Merging gender first year and first affiliation          
        df_gender_firstyear_firstaffiliation = df_gender_firstyear.\
                                                merge(df_affiliations, on='MatchMAGAID')
        
        
        # Extracting relevant columns for df_authors (dropping duplicates due to field)
        df_authors_relevant = df_authors_gt1000[['MAGAID',
                                                'GenderizeGender',
                                                'MAGFirstPubYear',
                                                'MAGFirstAffID']]\
                                                .drop_duplicates()
        
        # Matching based on gender and first year and first affiliation
        df_matched = df_authors_relevant\
                        .merge(df_gender_firstyear_firstaffiliation,
                        left_on=['GenderizeGender',
                                'MAGFirstPubYear',
                                'MAGFirstAffID'],
                        right_on=['MatchGenderizeGender',
                                'MatchMAGFirstPubYear',
                                'MatchMAGFirstAffID'],
                        how='left')
        
        # Let us check how many were matched.
        # Those matched will have MatchMAGAID as non null
        print("# Matched gt1000", 
                df_matched[~df_matched.MatchMAGAID.isna()]['MAGAID'].nunique())
        
        df_matched.to_csv(OUTDIR+"/RWMatched_gender_firstPubYear_gt1000firstAff.csv",
                        index=False)


def main():
        df_authors = pd.read_csv(INDIR_PROCESSED+"/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections_Augmented.csv",
                                usecols=['Record ID','MAGPID','MAGAID','GenderizeGender',
                                        'MAGFirstAffID', 'MAGFirstAffiliationRank','MAGFirstPubYear',
                                        'nRetracted', 'AttritedClass'])\
                                    .drop_duplicates()
        
        # Removing authors that were attrited and also those that have multiple offenses
        df_authors = df_authors[df_authors['nRetracted'].eq(1) &
                                df_authors['AttritedClass'].eq(0)]
        
        print("Number of authors undergoing matching", df_authors['MAGAID'].nunique())
        
        # First let us extract authors with affiliation rank in top 100
        top_100 = [str(rank) for rank in range(101)]
        df_authors_top100aff = df_authors[df_authors.MAGFirstAffiliationRank.isin(top_100)]
                
        # Now let us extract authors with affiliation rank between 100 and 1000
        df_authors_101to1000 = df_authors[~df_authors.MAGFirstAffiliationRank.isin(top_100) & 
                                                ~df_authors.MAGFirstAffiliationRank.eq('1001-')]

        # Now let us extract authors with affiliation rank above 1000
        df_authors_gt1000 = df_authors[df_authors.MAGFirstAffiliationRank.eq('1001-')]
        
        # Let us do a sensibility check here to check all authors are covered.
        # Note: There may be overlap of authors between three categories due to mult. affiliations
        assert(len(set(list(df_authors_top100aff.MAGAID.unique()) + \
                        list(df_authors_101to1000.MAGAID.unique()) + \
                        list(df_authors_gt1000.MAGAID.unique()))) == df_authors.MAGAID.nunique())
        
        # Now we shall deal with each of these three separately
        
        match_gender_firstyear_top100firstaff(df_authors_top100aff)
        
        #match_gender_firstyear_101to1000firstaff(df_authors_101to1000)
        
        #match_gender_firstyear_gt1000firstaff(df_authors_gt1000)

        print("done")
        
main()