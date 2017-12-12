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

    # format check
    formats = {table['format'] for table in report['tables']}
    results['goodtables:validate:format'] = ('Table is in format : ', logging.INFO, ', '.join(formats))
    # table count
    results['goodtables:table-count'] = ('Table count: ', logging.INFO, report['table-count'])

    error_rows = []
    for table in report['tables']:
        for error in table['errors']:
            if error['code'] == 'deviated-value':
                error_rows.append('median')
            if error['code'] == 'sequential-value':
                error_rows.append('sequential')

    # median
    if 'median' in error_rows:
        results['goodtables:median'] = ('Median value', logging.INFO, None)
    # sequential values
    if 'sequential' in error_rows:
        results['goodtables:sequential-values'] = ('Check for non-sequential values', logging.INFO, None)

    # full goodtables
    results['goodtables:all'] = ('Full analysis', logging.INFO, report)

    # returning results dict with checks
    return [results]


def run_validation(filename):
    return validate(filename, checks=[
        {'deviated-value': {'column': 'Mean', 'average': 'median', 'interval': 1}},
        {'sequential-value': {'column': 'ID'}}
    ])

def get_workflow(filename):
    workflow = {
        'validate': (run_validation, filename),
        'output': (check_csv, 'validate')
    }
    return workflow

if __name__ == "__main__":
    argv = sys.argv
    workflow = get_workflow(argv[1])
    print(get(workflow, 'output'))
