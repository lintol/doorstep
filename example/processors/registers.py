"""This script will attempt to validate any json register files from gov.uk
This will validate all government names e.g. council, country, street names
The first function will check to see if the json file provided to the function contains certain features
If they are contained, it is valid. If not, the json is not a valid register
Second function checks if ids are unique
Third function checks if ids are surjective i.e. are any ids missing?
Fourth function creates a dict object that loads the json file and runs each function and returns it."""


"""Imports json, numpy, pandas, and logging."""
import json
import numpy as np
import pandas as p
import logging
import sys



""" This is a dict object that contains categories commonly contained within a gov.uk register
 Data passed in will be checked against this in gov_register_checker
 This could be changed to suit different needs. """

check_agaisnt = {
    'c':'country',
    'on':'official-name',
    'n':'name',
    'cn':'citizen-names',
    'lat':'local-authority-type',
    'sd':'start-date',
    'ed':'end-date',
    't':'territory',
    'go':'government-organisation',
    'rd':'registration-district',
    'a':'area'
}

def gov_register_checker(data):
	# making test_data into data loaded from json
	test_data = json.loads(data)
	# for loop to iterate through items in json register data
	for key, check in test_data.items():
		# if statement to check if standards contained within check_agaisnt are contained in test_dump_data
		if check_agaisnt in test_data:	
			code = "reg_check_valid: reg-valid:{key}".format(key=key)
			report[code] = ("Register valid")
	return [report]
       

def check_register_id_unique(data):
    # setting up check for id...
    ids = data['index-entry-number']
    # report dict set up 
    report = {} 
    # min_duplicates checks if the length of the set of ids is lesser than the length of the ids
    min_duplicates = len(set(ids)) < len(ids)
    # if the min_duplicates var is more than 0 do this... 
    if min_duplicates > 0: 
        report['no_unique_ids'] = ('Warning: some duplicates were found!', logging.WARNING, '%d duplicate ids found' % (min_duplicates)) 
    return report  

def check_register_id_surjective(data):
    # setting up check for id....
    ids = data['index-entry-number']
    # setting up report as empty dict...
    report = {} 
    # unique_ids var is set to the result of the length function on the set of ids
    unique_ids = len(set(ids)) 
    # expected_ids is set to the sum of max amount of ids and the min amount of ids plus one
    expected_ids = max(ids) - min(ids) + 1
    # if the expected ids does not match the amount of unique ids do this... 
    if expected_ids != unique_ids: 
        report['missing'] = ('Warning: missing ids!', logging.WARNING, '%d missing ids' % (expected_ids - unique_ids, min(ids), max(ids)))  
    return report 

"""This is the workflow builder. This function will feed the json file into each method and then return the result"""
def return_workflow(file):
   # setting up workflow dict
    workflow = {

	'output_check' : (gov_register_checker, file),
	'output_id_unique': (check_register_id_unique, file),
	'output_sur': (check_register_id_surjective, file) 
    }
    return workflow 



if __name__ == "__main__":
    argv = sys.argv
    workflow = get_workflow(argv[1])
    print(get(workflow, 'output_check'))
    print(get(workflow, 'output_id_unique'))
    print(get(workflow, 'output_sur'))
