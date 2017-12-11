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
    for key, details in analysis.items():
        if details:
            code = 'check_pii_detail:pii-found:{key}'.format(key=key)
            report[code] = ('PII details found...', logging.INFO, details)

    return [report]

 
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
        'output': (return_report, filename)
    }

    # Returns workflow dict
    return workflow

if __name__ == "__main__":
    argv = sys.argv
    workflow = get_workflow(argv[1])
    print(get(workflow, 'output'))
