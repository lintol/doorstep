from goodtables import validate
import sys
from dask.threaded import get
import logging

def structure_report(report):
    results = {}

    if report['error-count']:
        results['goodtables:error-count'] = ('Errors found', logging.WARNING, report['error-count'])

    results['goodtables:table-count'] = ('Number of tables', logging.INFO, report['table-count'])
    results['goodtables:formats'] = ('Data formats', logging.INFO, ', '.join({table['format'] for table in report['tables']}))

    results['goodtables:all'] = ('Full Goodtables analysis', logging.INFO, report)

    return [results]

def get_workflow(filename):
    workflow = {
        'validate': (validate, filename),
        'output': (structure_report, 'validate')
    }
    return workflow

if __name__ == "__main__":
    argv = sys.argv
    workflow = get_workflow(argv[1])
    print(get(workflow, 'output'))
