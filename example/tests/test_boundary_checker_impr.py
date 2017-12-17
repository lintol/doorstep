import os
from dask.threaded import get

from processors.boundary_checker_impr import BoundaryCheckerImprovedProcessor

def test_boundary_checker_on_pedestrian_crossings():
    path = os.path.join('data', 'pedestriancrossing.geojson')
    boundary_checker = BoundaryCheckerImprovedProcessor()
    workflow = boundary_checker.get_workflow(path)
    results = get(workflow, 'output')
    assert len(results) == 1

    report = results[0]
    assert len(report) == 2
    assert report['locations_found'][2] == 1570
    assert report['locations_not_found'][2] == [1191, 1297]
