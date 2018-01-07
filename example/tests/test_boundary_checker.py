import os
from dask.threaded import get

from processors.boundary_checker import BoundaryCheckerProcessor

def test_pii_checker_on_pii():
    path = os.path.join('data', 'pedestriancrossing.geojson')
    boundary_checker = BoundaryCheckerProcessor()
    workflow = boundary_checker.get_workflow(path)
    results = get(workflow, 'output')
