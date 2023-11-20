#!/usr/bin/env python

"""
Summary:
This Python program focuses on extracting the academic age or 
the first year of publication for each retracted author 
listed in OpenAlex. Academic age is computed as the 
difference in the number of years between the retraction year 
and the first publication year. Since an author can have multiple
retractions, work_id and author_id is used a composite id for 
this task.
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