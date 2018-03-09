import os
from dask.threaded import get

from ltldoorstep_examples.pii import PiiProcessor

def test_pii_checker_on_pii():
    path = os.path.join('data', 'pii.csv')
    pii_checker = PiiProcessor()
    workflow = pii_checker.get_workflow(path)
    results = get(workflow, 'output')

    assert len(results) == 1

    report = results[0]
    assert report[0]['item']['entity']['location']['index'] == ['14 Naresuan Sq']
    assert report[0]['item']['entity']['location']['index'] == ['10.8.34.199', '127.0.34.199', '192.168.23.109']

