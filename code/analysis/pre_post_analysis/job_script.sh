#!/bin/bash                                                                                                                                         

##SBATCH -p bigmem #comment it out if you don't want to use the condo by adding extra #
#SBATCH -p compute
#SBATCH -n 1
##SBATCH -q css
##SBATCH -a 1-100
#SBATCH --mem=480GB #499GB is max when using condo #119GB doesn't require bigmem
#SBATCH -t  12:00:00 #times out after 48 hours
#SBATCH -o job.%J.geo.out  #output file
#SBATCH -e job.%J.geo.err  #input file                                                                                                                                
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sm9654@nyu.edu

#srun $(head -n $SLURM_ARRAY_TASK_ID jobs.txt | tail -n 1)
python3 1.collaborator_geo_analysis.py