# Lintol Processors

Included in this sub-tree are a set of processors that are
tested with Lintol Doorstep, for validating and/or cleaning
data. Sample datasets are provided to demonstrate functionality.

## Datasets

### pedestriancrossing.geojson

This dataset has been kindly made available by Northern Ireland's
Department for Infrastructure under the
[Open Government License](http://reference.data.gov.uk/id/open-government-licence).

The original dataset is available on the
[OpenDataNI portal page](https://www.opendatani.gov.uk/dataset/pedestrain-crossing).

### osni-ni-boundary.geojson

This dataset has been kindly made available by Northern Ireland's
Land and Property Services under the
[Open Government License](http://reference.data.gov.uk/id/open-government-licence).

The original dataset is available on the
[OpenDataNI portal page](https://www.opendatani.gov.uk/dataset?groups=property&license_id=uk-ogl&tags=Northern+Ireland&res_format=GeoJSON)

### osni-ni-boundary-lowres.geojson

This dataset has been derived from the `osni-ni-boundary.geojson` dataset, simplified by [mapshaper](http://mapshaper.org/).

## Tests

To test processors, run:

  python3 -m pytest test/test_NAME.py

in the /examples directory, with your virtual environment active.
