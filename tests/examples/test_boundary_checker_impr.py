import os
from dask.threaded import get
from ltldoorstep import report
import logging
from ltldoorstep import processor
import pytest
from collections import defaultdict

from ltldoorstep_examples.boundary_checker_impr import BoundaryCheckerImprovedProcessor


def test_boundary_checker_on_pedestrian_crossings():
    path = os.path.join('data', 'pedestriancrossing.geojson')
    boundary_checker = BoundaryCheckerImprovedProcessor()
    workflow = boundary_checker.get_workflow(path)
    get(workflow, 'output')
    results = boundary_checker.compile_report()
    assert results is not None
    assert type(results) is dict
    assert len(results) == 7

    report = results['tables'][0]['errors']
    assert report is not None
    assert type(report) is list
    assert len(report) == 2
    assert report[0]['item']['entity']['location']['index'] == 1191
    assert report[1]['item']['entity']['location']['index'] == 1297
