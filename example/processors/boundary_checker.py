"""Boundary checking

This function will find if certain geojson location data is within Northern Ireland.
If not, it reports this back to the user.
Make sure that you have a dataset of NI for the second argument.

"""

import sys
from geojson_utils import point_in_multipolygon
import numpy
import logging
from dask.threaded import get

DEFAULT_OUTLINE_GEOJSON = 'outline.geojson'


def find_ni_data(geojson, ni_json=None):

    if ni_json is None:
        ni_json = DEFAULT_OUTLINE_GEOJSON

    string_geojson = geojson.select_dtypes(include=['object'])
    dataset = set()
    data_str = '{type: multipolygon', string_geojson, '}'
    ni_data_str = {ni_json}
    is_data_in_ni = json.loads(data_str)
    check = point_in_multipolygon(is_data_in_ni, ni_data_str)
    string_geojson.apply(numpy.vectorize(lambda cell: dataset.update(check(c)for c in cell)))
    report = {}

    if None in dataset:
        dataset.remove(None)
        report['null_values'] = ("Null values found", logging.WARNING, None)

    report['found'] = ("Geographical data found from NI:", logging.INFO, ','.join(dataset))

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
