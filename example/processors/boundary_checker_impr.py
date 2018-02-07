"""Boundary checking (improved)

This function will (hopefully!) find if data in a csv file is contained within Northern Ireland.
If not so, this will be reported back to the user.
For now, please make sure that the second geojson in the argument is a boundary of Northern Ireland.

"""

import shapely
from geojson_utils import point_in_multipolygon
import logging
import json
from dask.threaded import get
import pandas as p
import geopandas as gp
import csv
import sys
from ltldoorstep.processor import DoorstepProcessor, geojson_add_issue, compile_report, set_properties

OUTLINES = {
    'GB-NIR': 'example/data/osni-ni-outline-lowres.geojson'
}


def find_ni_data(first_file, metadata=None):
    code = 'GB-NIR'
    if metadata and 'definitions' in metadata and metadata['definitions']:
        metadata = metadata['definitions'].values()[0]
        if metadata and \
                'configuration' in metadata and \
                'boundary' in metadata['configuration'] and \
                metadata['configuration']['boundary'] in OUTLINES:
            code = metadata['configuration']['boundary']
    ni_data = OUTLINES[code]

    with open(first_file) as data_file:
        data_to_compare = gp.GeoDataFrame.from_features(json.load(data_file)['features'])

    set_properties(preset='geojson', headers=list(data_to_compare.columns))
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
            for ix, point in outside_points.iterrows():
                geopoint = shapely.geometry.mapping(point['geometry'])
                props = dict(point)
                del props['geometry']
                geojson_add_issue(
                    'lintol/boundary-checker-improved:1',
                    logging.ERROR,
                    'locations-not-found',
                    _("This location is not within the given boundary"),
                    item_index=ix,
                    item=geopoint,
                    item_type=geopoint['type'],
                    item_properties=props
                )
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
            'output': (find_ni_data, filename, metadata)
        }
        return workflow

processor = BoundaryCheckerImprovedProcessor

if __name__ == "__main__":
    argv = sys.argv
    processor = BoundaryCheckerImprovedProcessor()
    workflow = processor.get_workflow(argv[1])
    print(compile_report(filename, metadata))
