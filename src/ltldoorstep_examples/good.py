from goodtables import validate
import sys
from dask.threaded import get
import logging
from ltldoorstep.processor import DoorstepProcessor, tabular_add_issue, set_properties

class GoodTablesProcessor(DoorstepProcessor):
    preset = 'tabular'
    code = 'frictionlessdata/goodtables-py:1'
    description = _("Processor wrapping Frictionless Data's Goodtables")

    def structure_report(report):
        results = {}

        levels = {
            'errors': logging.ERROR,
            'warnings': logging.WARNING,
            'informations': logging.INFO
        }

        for level, log_level in levels.items():
            table = report['tables'][0]
            set_properties(
                row_count=table['row-count'],
                headers=table['headers']
            )
            if level in table and table[level]:
                for error in table[level]:
                    tabular_add_issue(
                        log_level,
                        error['code'],
                        error['message'],
                        row_number=error['row-number'],
                        column_number=error['column-number'],
                        row=error['row']
                    )

        return report

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
