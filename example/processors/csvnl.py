from goodtables import validate
import logging
import sys
import pandas as p
# this function uses goodtables library in order to make sure that:
# the table is valid
# count the number of tables
# median values
# sequential values check
# full check with goodtables

def check_csv(report):

    results = {} # setting up results dictonary

    results['goodtables:validate:format'] = ('Table is in format : ', logging.INFO, report['format']) #format check
    results['goodtables:table-count'] = ('Table count: ', logging.INFO, report['table_count']) # table count
    results['goodtables:median'] = ('Median value', logging.INFO, report['median']) # median
    results['goodtables:sequential_values'] = ('Check for sequential values', logging.INFO, report['in_seqeunce']) # sequential values

    results['goodtables:all'] = ('Full analysis', logging.INFO, report) # full goodtables

    return [results] # returning results dict with checks


def get_workflow(filename):
    workflow = {
        'validate': (validate, filename),
        'output': (check_csv, 'validate')
    }
    return workflow

if __name__ == "__main__":
    argv = sys.argv
    print(check_csv(argv))
