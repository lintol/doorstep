"""This script will attempt to validate any json register files from gov.uk
This will validate all government names e.g. council, country, street names
The first function will check to see if the json file provided to the function contains certain features
If they are contained, it is valid. If not, the json is not a valid register
Second function checks if ids are unique
Third function checks if ids are surjective i.e. are any ids missing?
Fourth function creates a dict object that loads the json file and runs each function and returns it."""


"""Imports json, numpy, pandas, and logging. Goodtables is imported but commented out
It can be reimported if it is needed?"""
import json
import numpy as np
import pandas as p
import logging
# import goodtables  could possibly be needed to validate the register as a whole?


#This is a dict object that contains categories commonly contained within a gov.uk register
# Data passed in will be checked against this
# This could be changed
check_agaisnt={
    "c" : "country",
    "on" : "official-name",
    "n" : "name",
    "cn" : "citizen-names"
}


def gov_register_checker(data):
    string_json = json.load(data) # Loading in json data...
    valid = set() # setting var valid to function set


def check_register_id_unique(json):
    ids = json["index-entry-number"] # setting up check for id....
    report = {} # setting up report dict...
    min_duplicates = len(set(ids)) < len(ids) # min_duplicates checks if the length of the set of ids is lesser than the length of the ids
    if min_duplicates > 0: # If min_duplicates is more than 0 than do this....
        report['no_unique_ids'] = (logging.WARNING, '%d duplicate ids found' % min_duplicates) # if duplicates are found report becomes this
    return report  # report dict is returns

def check_register_id_surjective(json):
    ids = json["index-entry-number"] # setting up check for id...
    report = {} # setting up report dict
    unique_ids = len(set(ids)) # the unique ids are set to the length of the set of ids
    expected_ids = max(ids) - min(ids) + 1 # expected ids are min and max + 1 (n+1)
    if expected_ids != unique_ids: # If the amount of expected ids do not equal the amount of unique ids then do this...
        report['missing'] = (logging.WARNING, '%d missing ids' % (expected_ids - unique_ids, min(ids), max(ids))) # if missing ids are found do this....
    return report  # report dict return

def return_workflow(file):
    workflow = { # setting up dict for workflow
        'load-json': (p.read_json, file), # loading json
        'validate_json': (gov_register_checker, 'load-json'), # checking register
        'check_id_unique': (check_register_id_unique, 'load-json'), # checking unique
        'check_id_surjective' : (check_register_id_surjective, 'load-json'), # checking if ids are missing
        'output':  (list, ['validate-json', 'check_id_unique', 'check_id_surjective']) # output becomes list of all methods run
    }
    return workflow # returns workflow dict