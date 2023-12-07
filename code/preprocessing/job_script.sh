#!/bin/bash                                                                                                                                         

##SBATCH -p bigmem #comment it out if you don't want to use the condo by adding extra #
#SBATCH -p compute
#SBATCH -n 1
##SBATCH -q css
#SBATCH -a 1-60
#SBATCH --mem=480GB #499GB is max when using condo #119GB doesn't require bigmem
#SBATCH -t  24:00:00 #times out after 48 hours
#SBATCH -o job.%J.fuzz.out  #output file
#SBATCH -e job.%J.fuzz.err  #input file                                                                                                                                
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sm9654@nyu.edu

srun $(head -n $SLURM_ARRAY_TASK_ID jobs.txt | tail -n 1)
#python3 1a.extract_oa_works_rw.py
#python3 1b.merge_works_rw_oa_mag.py
#python3 2a.extract_oa_authors_rw.py
#python3 2b.extract_retraction_year.py
#python3 3.extract_oa_works_byRW_authors.py
#python3 4.extract_retraction_notices.py