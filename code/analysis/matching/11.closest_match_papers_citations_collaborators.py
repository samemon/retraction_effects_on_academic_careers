import pandas as pd
import numpy as np
import math
import random
from sklearn.preprocessing import StandardScaler

# INDIR = "/scratch/sm9654/retraction/RW_authors/rematched_pairs/parallel/090323/"
# OUTDIR = "/scratch/sm9654/retraction_effects_on_collaboration_networks/code/matching/closestMatch/nonattrited_matches/"

# directory where all my processed MAG files are
INDIR_DERIVED = "/scratch/bka3/Retraction_data/MAG/derived/"

# directory where retraction watch processed files are
INDIR_PROCESSED = "/scratch/sm9654/retraction_openalex/data/processed/"

# directory where we shall save all the new files
OUTDIR = "/scratch/sm9654/retraction_openalex/data/processed/author_matching/"

df_matched = pd.read_csv(OUTDIR+"/RWMatched_intersection_wPapersCitationsCollaboratorsAtRetraction_wCollabYear_wActivityPostRetraction.csv")\
                    .sort_values(by='MAGAID')

# Let us only extract rows where authors are not attrited.
# For that we shall first read the filtered sample
df_filtered = pd.read_csv(INDIR_PROCESSED+"/RW_authors_w_confounders_filteredSample_postNHB_BedoorsCorrections_Augmented.csv",
                        usecols=['MAGAID','AttritedClass', 'nRetracted']).drop_duplicates()

df_filtered_non_attrited = df_filtered[df_filtered.AttritedClass.eq(0) &
                                        df_filtered.nRetracted.eq(1)]

# Updating matched to only non-attrited
df_matched = df_matched[df_matched.MAGAID.isin(df_filtered_non_attrited.MAGAID)]

print("Number of matched retracted authors dealing with:", df_matched.MAGAID.nunique())

def get_stats(df_matched, key=['Record ID','MAGAID']):

        #grouped_size = df_matched.groupby(key).size()
        match_grouped_size = df_matched.groupby(key)['MatchMAGAID'].nunique()

        print("Total number of papers matched", df_matched['Record ID'].nunique())

        print("Authors matched",df_matched[~df_matched.MatchMAGAID.isna()].MAGAID.nunique())

        print("Average number of matches", match_grouped_size.mean())

        print("Minimum number of matches", match_grouped_size.min())

        print("Maximum number of matches", match_grouped_size.max())
                    
"""
We define the standadrized mean difference function 
to assess the covariate balance.
"""

def compute_smd_ate(df, key1, key2):
    numerator = df[key1].mean() - df[key2].mean()
    denominator = np.sqrt((df[key1].var() + df[key2].var())/2)
    return numerator/denominator

"""
We define the weighted euclidean distance function 
to compute the difference between treatment and 
matched control to later choose the one with the 
minimum distance.
"""

def compute_euclidean_distance(row, colsP1, colsP2, weights=[]):
    
    # Initializng 
    distance = 0
    assert(len(colsP1)==len(colsP2))

    # Going through all the elements
    for i in range(len(colsP1)):
        colP1 = row[colsP1[i]]
        colP2 = row[colsP2[i]]
        # Computing distance squared
        distance += (colP1-colP2)**2
        # Checking if weights are given
        if weights != []:
            # If so multiple by weights
            distance *= weights[i]
    
    # Return the sqrt of distance (unnecessary but just doing it for completion)
    return math.sqrt(distance)

def generate_weights():
    
    weightCombinations = []
    
    for i in range(1, 10):
        for j in range(1, 10):
            for k in range(1, 10):
                if (i + j + k == 10):
                    weightCombinations.append([round(i*0.1,1), round(j*0.1,1), round(k*0.1,1)])
    return weightCombinations

def choose_closestMatch(df, treatmentCol, distanceCol):
    
    df['MinDistance'] = df.groupby(treatmentCol)[distanceCol].transform(min)
    
    return df[df[distanceCol].eq(df.MinDistance)]

def choose_worstMatch(df, treatmentCol, distanceCol):
    
    df['MaxDistance'] = df.groupby(treatmentCol)[distanceCol].transform(max)
    
    return df[df[distanceCol].eq(df.MaxDistance)]

def choose_randomMatch(df, treatmentCol, controlCol, cols2consider):
    
    dfi = df[[treatmentCol,controlCol]+cols2consider].drop_duplicates()
        
    selected_pairs = []
    for magaid in dfi[treatmentCol].unique():
        matchmagaid = random.sample(dfi[dfi[treatmentCol]==magaid][controlCol].unique().tolist(), 1)
        selected_pairs.append((magaid, matchmagaid[0]))


    # Selecting rows with selected magaids only
    # NOTE: This could still have duplicates if one match has multiple affiliations.
    return dfi[dfi.apply(lambda row: (row[treatmentCol], row[controlCol]) in selected_pairs, axis=1)]


def choose_averageMatch(df, treatmentCol, controlCol, cols2consider):
    
    averageCols = ['Average'+col for col in cols2consider]
    
    dfi = df[[treatmentCol,controlCol]+cols2consider].drop_duplicates()
    
    # Each match should have only one entry (so multiple affiliations must be resolved although one match can occur for multiple retracted authors)
    assert(dfi.shape[0] == dfi[['MAGAID','MatchMAGAID']].drop_duplicates().shape[0])
    
    dfi[averageCols] = dfi.groupby('MAGAID')[cols2consider].transform(np.mean)
    
    # Now because affiliaton rank info is lost, let us merge new columns
    df_merged = df.merge(dfi[[treatmentCol,controlCol]+averageCols], on=[treatmentCol,controlCol])
    
    assert(df_merged.shape[0] == df.shape[0])
    
    return df_merged

def explore_closest_matching(tolerance = 0.2):
    
    # Defining treatment and control columns
    basicCols = ['MAGAID','MatchMAGAID', 'Record ID']

    #colsT = ['MAGCumPapers','MAGCumCitations','MAGCumCollaborators']
    colsT = ['MAGCumPapersAtRetraction','MAGCumCitationsAtRetraction','MAGCumCollaboratorsAtRetraction']

    #colsC = ['MatchMAGCumPapers','MatchMAGCumCitations','MatchMAGCumCollaborators']
    colsC = ['MatchMAGCumPapersAtRetraction','MatchMAGCumCitationsAtRetraction','MatchMAGCumCollaboratorsAtRetraction']
    
    # Now let us create the relevant dataset
    df_relevant = df_matched[basicCols+colsT+colsC].drop_duplicates()
    
    # Let us generate standardized columns first
    for i in range(len(colsT)):
        
        scaler = StandardScaler()
        df_relevant["Standardized"+colsT[i]] = scaler.fit_transform(df_relevant[[colsT[i]]])
        df_relevant["Standardized"+colsC[i]] = scaler.transform(df_relevant[[colsC[i]]])
    
    # Extracting possible weights for the three main confounders: papers, citations, collaborators
    weightLst = generate_weights()
    
    # Adding equal weight as part of the equation
    weightLst.append((1.0,1.0,1.0))
    
    # Initializing our dictionary from weights to SMDS
    weights_smdsRandom_smdsAverage = []
    
    # Let us compute square distances in advance
    # Let us compute square distances in advance
    df_relevant = df_relevant.assign(SqDiffPapers=(df_relevant['Standardized'+colsT[0]]-df_relevant['Standardized'+colsC[0]])**2, 
                                SqDiffCitations=(df_relevant['Standardized'+colsT[1]]-df_relevant['Standardized'+colsC[1]])**2, 
                                SqDiffCollaborators=(df_relevant['Standardized'+colsT[2]]-df_relevant['Standardized'+colsC[2]])**2)
    
    # Checking if distance is within x% for papers, cites, and collaborators

    
    df_relevant['papers_within_x_percent'] = (abs(df_relevant[colsC[0]] - df_relevant[colsT[0]]) <= (tolerance * df_relevant[colsT[0]]))   

    df_relevant['citations_within_x_percent'] = (abs(df_relevant[colsC[1]] - df_relevant[colsT[1]]) <= (tolerance * df_relevant[colsT[1]]))   
    
    df_relevant['collaborators_within_x_percent'] = (abs(df_relevant[colsC[2]] - df_relevant[colsT[2]]) <= (tolerance * df_relevant[colsT[2]]))
    
    # Removing all the rows where the above three conditions don't hold
    df_relevant = df_relevant[df_relevant['papers_within_x_percent'] & 
                            df_relevant['citations_within_x_percent'] & 
                            df_relevant['collaborators_within_x_percent']]
    
    print("After applying tolerance filter of",tolerance*100,"%")
    
    get_stats(df_relevant)
    
    df_relevant = df_relevant.assign(AbsDiffPapers=(df_relevant['Standardized'+colsT[0]]-df_relevant['Standardized'+colsC[0]])**2, 
                                AbsDiffCitations=(df_relevant['Standardized'+colsT[1]]-df_relevant['Standardized'+colsC[1]])**2, 
                                AbsDiffCollaborators=(df_relevant['Standardized'+colsT[2]]-df_relevant['Standardized'+colsC[2]])**2)
    
    # Going through each different weight combination
    for weights in weightLst:
        
        distanceCol = 'WEDPapersCitationsCollaborators'
        
        df_relevant[distanceCol] = weights[0] * df_relevant['SqDiffPapers'] + \
                                    weights[1] * df_relevant['SqDiffCitations'] + \
                                        weights[2] * df_relevant['SqDiffCollaborators']
        
        # Extracting the closest match based on the distance
        df_closest = choose_closestMatch(df_relevant, 'MAGAID', distanceCol)
        
        # Checking, how many scientists have more than one match even after this
        #print(df_closest.MatchMAGAID.nunique(),"Down from",df_relevant.MatchMAGAID.nunique())
        print("Weights explored", weights)
#         print("Authors with more than 1 match",
#               df_closest.groupby('MAGAID')['MatchMAGAID'].nunique().gt(1).value_counts().get(True,0))
        
        # Let us now create the average match
        df_closest_random = choose_randomMatch(df_closest, 'MAGAID', 'MatchMAGAID', colsT+colsC)
        
        # Let us now create the random match
        df_closest_average = choose_averageMatch(df_closest, 'MAGAID', 'MatchMAGAID', colsT+colsC)
        
#         print(df_closest_random.shape, df_closest_random.MAGAID.nunique(), 
#               df_closest_average.shape , df_closest.shape)
        
        # Saving
        if(weights[2] == 0.8):
            df_closest_average.to_csv(OUTDIR+"/closestAverageMatch_tolerance_"+str(round(tolerance,1))+"_w_0.8.csv",
                                    index=False)
            
        if(weights[2] == 1):
            df_closest_average.to_csv(OUTDIR+"/closestAverageMatch_tolerance_"+str(round(tolerance,1))+"_w_1.csv",
                                    index=False)
        
        """
        Both while choosing random or while choosing average, we did not consider the 
        affiliation rank. And so now we must choose the affiliation rank. But for now 
        let us forget the affiliation rank exists.
        """
        
        df_closest_random_i = df_closest_random[['MAGAID','MatchMAGAID',
                                                'MAGCumPapersAtRetraction', 'MatchMAGCumPapersAtRetraction',
                                                'MAGCumCitationsAtRetraction', 'MatchMAGCumCitationsAtRetraction',
                                                'MAGCumCollaboratorsAtRetraction', 'MatchMAGCumCollaboratorsAtRetraction']].\
                                                drop_duplicates()
        
        df_closest_average_i = df_closest_average[['MAGAID',
                                                'MAGCumPapersAtRetraction', 'AverageMatchMAGCumPapersAtRetraction',
                                                'MAGCumCitationsAtRetraction', 'AverageMatchMAGCumCitationsAtRetraction',
                                                'MAGCumCollaboratorsAtRetraction', 'AverageMatchMAGCumCollaboratorsAtRetraction']].\
                                                drop_duplicates()
        
        smd_papers_random = compute_smd_ate(df_closest_random_i, 'MAGCumPapersAtRetraction', 'MatchMAGCumPapersAtRetraction')
        smd_citations_random = compute_smd_ate(df_closest_random_i, 'MAGCumCitationsAtRetraction', 'MatchMAGCumCitationsAtRetraction')
        smd_collaborators_random = compute_smd_ate(df_closest_random_i, 'MAGCumCollaboratorsAtRetraction', 'MatchMAGCumCollaboratorsAtRetraction')
        
        smd_papers_average = compute_smd_ate(df_closest_average_i, 'MAGCumPapersAtRetraction', 'AverageMatchMAGCumPapersAtRetraction')
        smd_citations_average = compute_smd_ate(df_closest_average_i, 'MAGCumCitationsAtRetraction', 'AverageMatchMAGCumCitationsAtRetraction')
        smd_collaborators_average = compute_smd_ate(df_closest_average_i, 'MAGCumCollaboratorsAtRetraction', 'AverageMatchMAGCumCollaboratorsAtRetraction')
        
        
        smds_random = [smd_papers_random, smd_citations_random, smd_collaborators_random]
        smds_average = [smd_papers_average, smd_citations_average, smd_collaborators_average]
        
        smds_random = [round(i,3) for i in smds_random]
        smds_average = [round(i,3) for i in smds_average]
        
        weights_smdsRandom_smdsAverage.append([tuple(weights)]+smds_random+smds_average)
    
    return weights_smdsRandom_smdsAverage

for tolerance in np.linspace(0,1,10,endpoint=False)[1:]:

    weights_smdsRandom_smdsAverage = explore_closest_matching(tolerance)

    df_smds = pd.DataFrame(weights_smdsRandom_smdsAverage, 
                    columns =['(wPapers,wCites,wCollabs)', 
                                'SMDpapersRandom', 'SMDCitationsRandom', 'SMDCollaboratorsRandom',
                                'SMDpapersAverage', 'SMDCitationsAverage', 'SMDCollaboratorsAverage',])

    print(df_smds)

    df_smds.to_csv(OUTDIR+"/SMDs/weights_to_SMDs_rematching_"+str(int(tolerance*100))+".csv", index=False)