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
from ltldoorstep.processor import DoorstepProcessor

DEFAULT_OUTLINE_GEOJSON = 'data/osni-ni-outline-lowres.geojson'


def find_ni_data(first_file, ni_data=None):
    report = {}

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

        # If the points in the csv data match/are contained in the NI data json, do this...
        if inside_points_ct:
            # Reports back to the user that data is in NI
            report['locations_found'] = \
                ('Locations in Northern Ireland', logging.INFO, inside_points_ct)

        # If data in the first csv file are not contained in the second NI geojson file do this...
        if not outside_points.empty:
            # Report dict warns user that location is not in NI
            report['locations_not_found'] = \
                ('Locations not in Northern Ireland', logging.WARNING, list(outside_points.index))
    # If the csv file does not have any location data....
    else:
        report['no_loc_data_found'] = ('No location data found! Please make sure that you have read the right file', logging.WARNING)

    return [report]

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
