#!/bin/bash                                                                                                                                         

##SBATCH -p bigmem #comment it out if you don't want to use the condo by adding extra #
#SBATCH -p compute
#SBATCH -n 1
##SBATCH -q css
#SBATCH -a 1-100
#SBATCH --mem=400GB #499GB is max when using condo #119GB doesn't require bigmem
#SBATCH -t  48:00:00 #times out after 48 hours
#SBATCH -o job.%J.tc.out  #output file
#SBATCH -e job.%J.tc.err  #input file                                                                                                                                
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sm9654@nyu.edu

srun $(head -n $SLURM_ARRAY_TASK_ID jobs.txt | tail -n 1)

#python3 1.gender_firstyear_firstaff.py
#python3 merge_firstaff_files.py
#python3 3.remove_1Dcollaborators.py
#python3 4.retractionaff.py
#python3 4.discipline.py
#python3 5.intersection.py
#python3 6.papers_citations_collaborators_corrected.py
#python3 7.extracting_collaborator_edges.py
#python3 8.remove_authors_wo_collabYear.py
#python3 9.extracting_activity_AllMatchesUntilCategorical.py
#python3 10.filter_authors_and_multiple_aff.py
#python3 11.closest_match_papers_citations_collaborators.py
# python3 12.process_1d_collabs.py
#python3 16.extracting_2D_collaborators_closestmatches_rematching.py