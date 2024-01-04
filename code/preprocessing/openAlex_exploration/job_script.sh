#!/bin/bash                                                                                                                                         

##SBATCH -p bigmem #comment it out if you don't want to use the condo by adding extra #
##SBATCH -p compute
#SBATCH -n 1
#SBATCH -q bedoor
#SBATCH --mem=800GB #499GB is max when using condo #119GB doesn't require bigmem
#SBATCH -t  48:00:00 #times out after 48 hours
#SBATCH -o job.%J.MAGvsOA.out  #output file
#SBATCH -e job.%J.MAGvsOA.err  #input file                                                                                                                                
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sm9654@nyu.edu


python3 0b.difference_between_MAG_OA.py
