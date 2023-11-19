#!/bin/bash                                                                                                                                         

#SBATCH -p bigmem #comment it out if you don't want to use the condo by adding extra #
##SBATCH -p compute
#SBATCH -n 1
##SBATCH -q css
##SBATCH -a 1-100
#SBATCH --mem=1800GB #499GB is max when using condo #119GB doesn't require bigmem
#SBATCH -t  09:00:00 #times out after 48 hours
#SBATCH -o job.%J.openalex.out  #output file
#SBATCH -e job.%J.openalex.err  #input file                                                                                                                                
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sm9654@nyu.edu

#srun $(head -n $SLURM_ARRAY_TASK_ID jobs2.txt | tail -n 1)
python3 extract_oa_works_rw.py
#python3 extract_oa_authors_rw.py