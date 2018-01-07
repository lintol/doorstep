from goodtables import validate
import sys
from dask.threaded import get
import logging
from ltldoorstep.processor import DoorstepProcessor

def structure_report(report):
    results = {}

    if report['error-count']:
        results['goodtables:error-count'] = ('Errors found', logging.WARNING, report['error-count'])

    results['goodtables:table-count'] = ('Number of tables', logging.INFO, report['table-count'])

    tables = report['tables']

    formats = {table['format'] for table in tables}
    results['goodtables:formats'] = ('Data formats', logging.INFO, ', '.join(formats))

    results['goodtables:all'] = ('Full Goodtables analysis', logging.INFO, report)

    return [results]

class GoodTablesProcessor(DoorstepProcessor):
    def get_workflow(self, filename, metadata={}):
        workflow = {
            'validate': (validate, filename),
            'output': (structure_report, 'validate')
        }
        return workflow

processor = GoodTablesProcessor

if __name__ == "__main__":
    argv = sys.argv
    processor = GoodTablesProcessor()
    workflow = processor.get_workflow(argv[1])
    print(get(workflow, 'output'))
