import os
from dask.threaded import get
from ltldoorstep_examples.registers import processor
import pytest
import json

def testing_register_pass():
    path = os.path.join(os.path.dirname(__file__), 'data', 'registers-sample.csv')
    registers = processor()
    workflow = registers.get_workflow(path)
    report = get(workflow, 'output')
    results = report.compile()

    print(json.dumps(report.compile(), indent=True))
    assert len(results['tables'][0]['warnings']) == 5

    report = [issue for issue in results['tables'][0]['warnings'] if issue['message'].endswith('\'FY\'')].pop()
    assert report['code'] == 'country-mismatch'
    assert report['error-data']['mismatch'] == 'FY'
    assert len(report['context']) == 1
    assert report['context'][0]['properties'] == {
        'name': 'Venus MacNee',
        'nationality': 'Namibian',
        'sample_date': '2017-03-01',
        'state': 'FY',
        'organizations': 12,
        'country': 'Finland'
    }
    assert len(report['error-data']) == 1

    report = [issue for issue in results['tables'][0]['warnings'] if issue['message'].endswith('\'British\'?')].pop()
    assert report['code'] == 'country-mismatch'
    assert report['error-data'] == {
        'mismatch': 'Britidh',
        'guess': ('British', 86)
    }
    assert len(report['context']) == 1
    assert report['context'][0]['properties'] == {
        'country': 'Trinidad',
        'nationality': 'Britidh',
        'sample_date': '2016-03-23',
        'name': 'Cara Matisz',
        'state': 'TT',
        'organizations': 9
    }
    assert report['item']['entity'] == {
        'type': 'Cell',
        'definition': None,
        'location': {
            'row': 3,
            'column': 5
        }
    }

    assert len(results['tables'][0]['informations']) == 3
    report = results['tables'][0]['informations'][0]
    assert report['code'] == 'country-checked'
    assert report['item']['entity']['type'] == 'Column'
