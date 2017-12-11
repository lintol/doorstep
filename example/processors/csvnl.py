"""Goodtables processor (improved)

This function uses goodtables library in order to make sure that:
 * the table is valid
 * count the number of tables
 * median values
 * sequential values check
 * full check with goodtables

"""

from goodtables import validate
import logging
import sys
import pandas as p
from dask.threaded import get

def check_csv(report):
    # setting up results dictonary
    results = {}

    if ni_data is None:
        ni_data = DEFAULT_OUTLINE_GEOJSON

    # format check
    results['goodtables:validate:format'] = ('Table is in format : ', logging.INFO, report['format'])
    # table count
    results['goodtables:table-count'] = ('Table count: ', logging.INFO, report['table_count'])
    # median
    results['goodtables:median'] = ('Median value', logging.INFO, report['median'])
    # sequential values
    results['goodtables:sequential_values'] = ('Check for sequential values', logging.INFO, report['in_seqeunce'])

    # full goodtables
    results['goodtables:all'] = ('Full analysis', logging.INFO, report)

    # returning results dict with checks
    return [results]


def get_workflow(filename):
    workflow = {
        'validate': (validate, filename),
        'output': (check_csv, 'validate')
    }
    return workflow

if __name__ == "__main__":
    argv = sys.argv
    workflow = get_workflow(argv[1])
    print(get(workflow, 'output'))
