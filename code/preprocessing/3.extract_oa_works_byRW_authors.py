#!/usr/bin/env python

"""
Summary:
This Python program focuses on extracting all 
the works by each retracted author along with each 
paper's publication year, title, venue, and type. 
This will help us compute downstream variables such 
as academic age, # of papers prior to retraction, 
number of collaborators, and so on.
"""

import pandas as pd
import configparser
import os

def read_config():
    # preprocessing the config file
    config = configparser.ConfigParser()
    config.read('preprocessing_config.ini')
    return config['Paths']

def main():
    # reading all the relevant paths
    paths = read_config()
    OUTDIR = paths['OUTDIR']
    OA_WORKS_AUTHORSHIPS_PATH = paths['OA_WORKS_AUTHORSHIPS_PATH']
    
    

if __name__ == "__main__":
    main()