import os
from dask.threaded import get
from ltldoorstep_examples.registers import RegisterCountryProcessor
import pytest

def testing_register_pass():
    path = os.path.join('data', 'registers-sample.csv')
    registers = RegisterCountryProcessor()
    workflow = registers.get_workflow(path)
    results = get(workflow, 'output')

    assert len(results)==1
    report = results[0]
    assert report['country-checked'][2] == {'state': 'country-register-country', 'country': 'country-register-name', 'nationality': 'country-register-citizen-names'}
    assert report['country-mismatch'][2]['state'] == {2: {'mismatch': 'FY'}}
    assert report['country-mismatch'][2]['country'] == {
        1: {'mismatch': 'Untied Knigdom', 'guess': ('United Kingdom', 86)},
        3: {'mismatch': 'Trinidad', 'guess': ('Trinidad and Tobago', 100)},
        4: {'mismatch': 'Trinidad', 'guess': ('Trinidad and Tobago', 100)}
    }
    assert report['country-mismatch'][2]['nationality'] == {3: {'mismatch': 'Britidh', 'guess': ('British', 86)}}
