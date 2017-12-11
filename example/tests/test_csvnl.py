import os
from dask.threaded import get

from processors import csvnl

def test_csvnl_on_bad():
    path = os.path.join('data', 'bad.csv')
    workflow = csvnl.get_workflow(path)
    results = get(workflow, 'output')

    assert len(results) == 1

    report = results[0]
    assert 'goodtables:error-count' not in report
    assert report['goodtables:table-count'][2] == 1
    assert report['goodtables:formats'][2] == 'csv'
