"""Boundary checking (improved)

This function will (hopefully!) find if data in a csv file is contained within Northern Ireland.
If not so, this will be reported back to the user.
For now, please make sure that the second geojson in the argument is a boundary of Northern Ireland.

"""

from geojson_utils import point_in_multipolygon
import logging
import json
from dask.threaded import get
import pandas as p
import geopandas as gp
import csv
import sys
from ltldoorstep.processor import DoorstepProcessor, geojson_add_issue

DEFAULT_OUTLINE_GEOJSON = 'data/osni-ni-outline-lowres.geojson'


def find_ni_data(first_file, ni_data=None):
    if ni_data is None:
        ni_data = DEFAULT_OUTLINE_GEOJSON

    data_to_compare = gp.read_file(first_file)

    # If csv file has these attributes then...
    if 'geometry' in data_to_compare:

        # TODO: could put a check here if file is csv or geojson...
        # need to do this later.

        # This is the second geojson file that should be NI boundaries.
        # This is what we are comparing the first csv/json file to.
        ni_compare_data = gp.read_file(ni_data)

        # This var contains the output of the function point_in_multipolygon.
        # This should contain a bool.
        multipolygon = ni_compare_data.ix[0]['geometry']
        points = data_to_compare['geometry']
        outside_points = data_to_compare[[not multipolygon.contains(p) for p in points]]
        inside_points_ct = len(points) - len(outside_points)

        # If data in the first csv file are not contained in the second NI geojson file do this...
        if not outside_points.empty:
            # Report dict warns user that location is not in NI
            for ix in outside_points.index:
                geojson_add_issue('lintol/boundary-checker-improved:1', logging.WARNING, 'locations-not-found', _("This location is not within the given boundary"), item_index=ix)
    # If the csv file does not have any location data....
    else:
        geojson_add_issue(
            'lintol/boundary-checker-improved:1',
            logging.WARNING,
            'no-location-data-found',
            _("No location data found! Please make sure that you have read the right file")
        )

class BoundaryCheckerImprovedProcessor(DoorstepProcessor):
    def get_workflow(self, filename, metadata={}):
        workflow = {
            'output': (find_ni_data, filename)
        }
        return workflow

processor = BoundaryCheckerImprovedProcessor

if __name__ == "__main__":
    argv = sys.argv
    processor = BoundaryCheckerImprovedProcessor()
    workflow = processor.get_workflow(argv[1])
    print(get(workflow, 'output'))
