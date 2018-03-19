import os
from dask.threaded import get
from ltldoorstep_examples.registers import processor
import pytest

def testing_register_pass():
    path = os.path.join(os.path.dirname(__file__), 'data', 'registers-sample.csv')
    registers = processor()
    workflow = registers.get_workflow(path)
    report = get(workflow, 'output')
    results = report.compile()

    print(report)
    assert len(results['tables'][0]['warnings']) == 1
    report = results['tables'][0]['warnings'][0]
    assert report['code'] == 'country-mismatch'
    assert report[2]['state'] == {2: {'mismatch': 'FY'}}
    assert report[2]['country'] == {
        1: {'mismatch': 'Untied Knigdom', 'guess': ('United Kingdom', 86)},
        3: {'mismatch': 'Trinidad', 'guess': ('Trinidad and Tobago', 100)},
        4: {'mismatch': 'Trinidad', 'guess': ('Trinidad and Tobago', 100)}
    }
    assert report[2]['nationality'] == {3: {'mismatch': 'Britidh', 'guess': ('British', 86)}}

    assert len(results['tables'][0]['informations']) == 1
    report = results['tables'][0]['informations'][0]
    assert report['code'] == 'country-checked'
    assert report[2] == {'state': 'country-register-country', 'country': 'country-register-name', 'nationality': 'country-register-citizen-names'}
