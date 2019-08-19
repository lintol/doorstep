from goodtables import validate
from ltldoorstep import regex_utils as utils
import numpy as np
import re
import yaml
import dateparser
from ltldoorstep.reports import report
import pandas as p
import sys
from dask.threaded import get
import logging
from ltldoorstep.processor import DoorstepProcessor
import json

MATCH_RATIO = 0.05
MAX_SAMPLE_SIZE = 1000

def json_loader(filename):
    with open(filename, 'r') as f:
        read = json.load(f)
    return read

def json_walk(iterable, matcher, match, total, skip_it=None):
    for elt in iterable:
        if isinstance(elt, tuple):
            key = elt[0]
            elt = elt[1]
        else:
            key = None

        if isinstance(elt, list):
            match, total = json_walk(elt, matcher, match, total, skip_it)
        elif isinstance(elt, dict):
            match, total = json_walk(elt.items(), matcher, match, total, skip_it)
        else:
            if skip_it is not None:
                if next(skip_it):
                    total += 1
                    if matcher and matcher(elt, key):
                        match += 1
            else:
                total += 1

    return match, total

def is_this_spatial_csv(data, metadata):
    keywords = {'lat', 'lng', 'northing', 'easting', 'latitude', 'longitude', 'coordinates', 'coord'}
    value_res = {utils.get_regex('uk-postcode'), utils.get_regex('common-street-indicators')}

    match = 0
    total = 0

    key_matcher = lambda k: isinstance(k, str) and any({kw in k.lower() for kw in keywords})
    header_match = len([d for d in data.columns if key_matcher(d)])

    matcher = lambda x: isinstance(x, str) and any({rex.search(x.lower()) for rex in value_res})

    data_match = 0
    data_sample = data.sample(MAX_SAMPLE_SIZE) if MAX_SAMPLE_SIZE < len(data) else data
    for ix, row in data_sample.iterrows():
        if any([matcher(x) for x in row]):
            data_match += 1
        total += len(row)

    return 30 * header_match + 10 * data_match, total

def is_this_spatial_json(data, metadata):
    keywords = {'lat', 'lng', 'northing', 'easting', 'latitude', 'longitude', 'coordinates', 'coord'}

    _, total = json_walk([data], None, 0, 0, None)

    skip = np.full((total,), False)
    skip[np.random.choice(total, size=min(total, MAX_SAMPLE_SIZE), replace=False)] = True

    matcher = lambda x, k: isinstance(k, str) and any({kw in k.lower() for kw in keywords})

    match, total = json_walk([data], matcher, 0, 0, iter(skip))

    return match, total

re_likely_datetime = utils.get_regex('search-basic-date-time')
def is_datetime(x, k):
    return isinstance(x, str) and len(x) > 3 and re_likely_datetime.search(x) and dateparser.parse(x) is not None

def is_datetime_deep(x, k):
    return isinstance(x, str) and len(x) > 3 and dateparser.parse(x) is not None

def is_this_timeseries_csv(data, metadata):
    keywords = {'year', 'month', 'hour', 'day', 'minute'}

    match = 0
    total = 0

    matcher = lambda k: isinstance(k, str) and any({kw in k.lower() for kw in keywords})
    header_match = len([d for d in data.columns if matcher(d)])

    sample_row_names = data.index[np.random.choice(total, size=min(total, int(MAX_SAMPLE_SIZE / 10)), replace=False)]
    row_match = len([d for d in sample_row_names if is_datetime_deep(d, None)])

    data_match = 0
    data_sample = data.sample(MAX_SAMPLE_SIZE) if MAX_SAMPLE_SIZE < len(data) else data
    for ix, row in data_sample.iterrows():
        if any([is_datetime(x, k) for k, x in row.items()]):
            data_match += 1
        total += len(row)

    sqrt_sample = int(np.sqrt(MAX_SAMPLE_SIZE))
    data_sample = data.sample(sqrt_sample) if sqrt_sample < len(data) else data
    for ix, row in data_sample.iterrows():
        sample = row.sample(sqrt_sample) if len(row) > sqrt_sample else row
        if any([is_datetime_deep(x, k) for k, x in sample.items()]):
            data_match += 1
        total += len(sample)

    return 100 * row_match + 30 * header_match + 10 * data_match, total

def is_this_timeseries_json(data, metadata):
    matcher = is_datetime

    _, total = json_walk([data], None, 0, 0, None)

    skip = np.full((total,), False)
    skip[np.random.choice(total, size=min(total, MAX_SAMPLE_SIZE), replace=False)] = True

    match, total = json_walk([data], matcher, 0, 0, iter(skip))

    return match, total

class DtComprehenderProcessor(DoorstepProcessor):
    preset = 'tabular'
    code = 'datatimes/dt-comprehender:1'
    description = _("Infer the nature of the data searched")

    def structure_report(self, report):
        results = {}

        levels = {
            'errors': logging.ERROR,
            'warnings': logging.WARNING,
            'informations': logging.INFO
        }

        table = report['tables'][0]
        self._report.set_properties(
            row_count=table['row-count'],
            headers=table['headers']
        )
        for level, log_level in levels.items():
            if level in table and table[level]:
                for error in table[level]:
                    row_number = error['row-number'] if 'row-number' in error else None
                    column_number = error['column-number'] if 'column-number' in error else None
                    row = error['row'] if 'row' in error else None

                    self._report.add_issue(
                        log_level,
                        error['code'],
                        error['message'],
                        row_number=row_number,
                        column_number=column_number,
                        row=row
                    )

        return self._report

    @staticmethod
    def make_report():
        return report.TabularReport(
            'datatimes/dt-comprehender:1',
            _("Data Times processor to infer the nature of a dataset")
        )

    def get_workflow(self, filename, metadata):
        # setting up workflow dict
        if filename.endswith('csv'):
            spatial = is_this_spatial_csv
            timeseries = is_this_timeseries_csv
            read = lambda x: p.read_csv(x, low_memory=False)
        elif filename.endswith('geojson'):
            spatial = lambda *args: (1, 1)
            read = json_loader
            timeseries = is_this_timeseries_json
        elif filename.endswith('json'):
            spatial = is_this_spatial_json
            timeseries = is_this_timeseries_json
            read = json_loader
        else:
            # Null response
            return {'output': (lambda x: x, self._report)}

        workflow = {
            'data': (read, filename),
            'spatial': (spatial, 'data', self.metadata),
            'timeseries': (timeseries, 'data', self.metadata),
            'output': (classify, self._report, 'spatial', 'timeseries')
        }
        return workflow

def classify(rprt, spatial_pair, timeseries_pair):
    types = {
        'spatial': spatial_pair,
        'timeseries': timeseries_pair
    }
    tags = []

    for typ, (match, total) in types.items():
        if total < 1e-10:
            continue

        include = match / total > MATCH_RATIO

        if include:
            tags.append(typ)
        types[typ] = (match, total, include)

        rprt.add_issue(
            logging.INFO,
            f'comprehension-type-{typ}',
            _("The {} match ratio is {} = {} / {}").format(typ, match / total, match, total),
            error_data=(match, total, MATCH_RATIO)
        )

    rprt.add_issue(
        logging.INFO,
        'all-comprehension-type-tags',
        _("This dataset appears to be: {}").format(', '.join(tags)),
        error_data=tags,
        at_top=True
    )

processor = DtComprehenderProcessor.make

if __name__ == "__main__":
    argv = sys.argv
    processor = DtComprehenderProcessor()
    workflow = processor.build_workflow(argv[1])
    print(get(workflow, 'output'))
