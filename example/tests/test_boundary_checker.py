import os
from dask.threaded import get

from processors import boundary_checker as boundary_checker

def test_pii_checker_on_pii():
    path = os.path.join('data', 'pedestriancrossing.geojson')
    workflow = boundary_checker.get_workflow(path)
    results = get(workflow, 'output')
