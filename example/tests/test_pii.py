import os
from dask.threaded import get

from processors import test as pii_checker

def test_pii_checker_on_pii():
    path = os.path.join('data', 'pii.csv')
    workflow = pii_checker.get_workflow(path)
    results = get(workflow, 'output')

    assert len(results) == 1

    report = results[0]
    assert 'goodtables:error-count' not in report
    assert report['goodtables:table-count'][2] == 1
    assert report['goodtables:formats'][2] == 'csv'
