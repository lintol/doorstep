import os
from dask.threaded import get

from processors.csvnl import GoodTablesProcessor

def test_csvnl_on_bad():
    path = os.path.join('data', 'bad.csv')
    csvnl = GoodTablesProcessor()
    workflow = csvnl.get_workflow(path)
    results = get(workflow, 'output')

    assert len(results) == 1

    report = results[0]
    assert 'goodtables:error-count' not in report
    assert report['goodtables:table-count'][2] == 1
    assert report['goodtables:validate:format'][2] == 'csv'
    assert report['goodtables:median'][2] is None
    assert report['goodtables:sequential-values'][2] is None
