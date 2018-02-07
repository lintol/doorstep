"""Boundary checking (improved)

This function will (hopefully!) find if data in a csv file is contained within Northern Ireland.
If not so, this will be reported back to the user.
For now, please make sure that the second geojson in the argument is a boundary of Northern Ireland.

Note: this particular version of this processor is for testing the Report object

"""

from geojson_utils import point_in_multipolygon
import logging
import json
from dask.threaded import get
import pandas as p
import geopandas as gp
import csv
import sys
import report as r


DEFAULT_OUTLINE_GEOJSON = 'data/osni-ni-outline-lowres.geojson'


# constructor for processor - takes in report object and processor object
def __init__(self, processor, report):
    self.processor = processor
    self.report = report


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

        # If the points in the csv data match/are contained in the NI data json, do this...
        if inside_points_ct:
            # Reports back to the user that data is in NI

            report = reporting.r.add_issue('locations found', 'Locations in NI', logging.WARNING, inside_points_ct)

        # If data in the first csv file are not contained in the second NI geojson file do this...
        if not outside_points.empty:
            # Report dict warns user that location is not in NI
                report = reporting.r.add_issue('locations found not in ni', 'Locations in NOT in NI', logging.WARNING, list(outside_points.index))

    else:
        report = self.r.add_issue('no location data found', 'No location data found in file', logging.WARNING)

    return report


def get_workflow(filename):
    workflow = {
        'output': (find_ni_data, filename)
    }
    return workflow


if __name__ == "__main__":
    argv = sys.argv
    workflow = get_workflow(argv[1])
    print(get(workflow, 'output'))
