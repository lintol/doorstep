"""PII Checker

This function will return the results of a csv file to find
personally identifable info(PII)

"""

from piianalyzer.analyzer import PiiAnalyzer
import logging
import numpy as np
import pandas as p
from dask.threaded import get
import sys

pii_details = {
    'N': 'name',
    'P': 'phone_no',
    'A': 'address',
    'C': 'credit_card_no',
    'E': 'email_address',
    'O': 'organization'
}

def return_report(csv):
    # Feeding in csv file to PIIAnalyzer...
    piianalyzer = PiiAnalyzer(csv)

    # Set dataset object to set method
    dataset = set()

    # Assigning the results of the analysis to analysis var...
    analysis = piianalyzer.analysis()

    # Setting up report dict....
    report = {}

    np.vectorize(lambda cell: dataset.update({analysis(a)for a in cell}))

    return {
        'check_pii_detail:pii-found': ('PII details found...', logging.INFO, [pii_details[d[0]] for d in dataset])
    }

 
def get_workflow(filename):
    """Workflow builder

    This function will return the workflow taken from return_report,
    feeds a csv file into return_report

    """

    # Setting up workflow dict...
    #  loading: Using Pandas library to read csv file passed in,
    #           and also pass the filename
    #  report_returned: Using return_report and loading csv file....
    #  output: This will be the output
    workflow = {
        'loading': (p.read_csv, filename),
        'report_returned': (return_report, 'loading'),
        'output': (list, ['report_returned'])
    }

    # Returns workflow dict
    return workflow

if __name__ == "__main__":
    argv = sys.argv
    workflow = get_workflow(argv[1])
    print(get(workflow, 'output'))
