"""Testing for report functionality"""

import json
import pytest
from ltldoorstep.printer import TermColorPrinter, JsonPrinter
import logging


@pytest.fixture()
def report():
    return {
        "filename": "test.csv",
        "supplementary": [],
        "time": 0.1,
        "preset": "tabular",
        "tables": [{
            "row-count": 10,
            "format": "csv",
            "errors": [
                {
                    "code": "test-error",
                    "message": "TEST",
                    "processor": "test-proc",
                    "item": {
                        "entity": {
                            "type": "Cell",
                            "location": {},
                            "definition": None
                        },
                        "properties": {}
                    },
                    "context": []
                }
            ],
            "warnings": [],
            "informations": []
        }]
    }

def test_term_color_printer(report):
    """check printing to terminal works"""

    printer = TermColorPrinter()
    printer.build_report(report)

    assert 'Errors' in printer.get_output()
    assert 'Warnings' not in printer.get_output()
    assert 'test-proc  {}  test-error  TEST  None' in printer.get_output()

def test_json_printer(report):
    """check building of JSON reports"""

    printer = JsonPrinter()
    printer.build_report(report)

    assert json.loads(printer.get_output()) == report
