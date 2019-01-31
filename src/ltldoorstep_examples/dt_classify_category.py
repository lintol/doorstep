"""This script will attempt to classify any files by semantic location

"""

import json
import gettext
import pandas as p
import logging
import sys
import os
import ltldoorstep
import gettext
from ltldoorstep.processor import DoorstepProcessor
from ltldoorstep.reports import report
from ltldoorstep.location_utils import load_berlin
from dask.threaded import get
import requests

import json

METADATA_ROWS = {
    'name': lambda x: [x['name']] if 'name' in x else [],
    'notes': lambda x: [x['notes']],
    'resource name': lambda x: [r['name'] for r in x['resources'] if 'name' in r],
    'resource description': lambda x: [r['description'] for r in x['resources']],
    'group title': lambda x: [r['title'] for r in x['groups']],
    'tags': lambda x: [r['name'] for r in x['tags']],
    'topicCategories': lambda x: x['topic_category'] if 'topic_category' in x else []
}

def get_sentences_from_metadata(metadata):
    data_lines = []
    pkg_metadata = metadata.package

    for k, extractor in METADATA_ROWS.items():
        data_lines += [(k, v) for v in extractor(pkg_metadata)]

    return data_lines

def get_categories(sentences):
    result = requests.post('http://localhost:8000/', json=sentences)

    if result.status_code == 400:
        raise RuntimeError(_("Malformed category request: ") + result.content.decode('utf-8'))
    elif result.status_code != 200:
        raise RuntimeError(_("Could not access category service"))

    return result.json()

def get_sentences_from_data(csv):
    return []

def classify_sentences(rprt, data_sentences, metadata_sentences):
    sentences = data_sentences + metadata_sentences

    keys, sentences = zip(*sentences)
    results = get_categories(sentences)

    for key, result in zip(keys, results):
        print(result)
        issue_text = _("Possible categories in {}: {}").format(key, ", ".join(["{} ({:.2f}%)".format(_(c), r * 100) for r, c in result]))

        rprt.add_issue(
            logging.INFO,
            'possible-categories-overall',
            issue_text,
            error_data=results,
            at_top=True
        )

    return rprt


class DTClassifyCategoryProcessor(DoorstepProcessor):
    @staticmethod
    def make_report():
        return report.TabularReport(
            'datatimes/classify-category:1',
            _("Data Times Category Classification Processor")
        )

    def get_workflow(self, filename, metadata={}):
        # setting up workflow dict
        workflow = {
            'load_csv': (p.read_csv, filename),
            'sentences_from_data': (get_sentences_from_data, 'load_csv'),
            'sentences_from_metadata': (get_sentences_from_metadata, self.metadata),
            'output': (classify_sentences, self._report, 'sentences_from_data', 'sentences_from_metadata')
        }
        return workflow

processor = DTClassifyCategoryProcessor.make

if __name__ == '__main__':
    gettext.install('ltldoorstep')
    proc = processor()

    metadata = {}
    if len(sys.argv) > 2:
        with open(sys.argv[2], 'r') as metadata_f:
            metadata = json.load(metadata_f)

    workflow = proc.build_workflow(sys.argv[1], metadata)
    print(get(workflow, 'output'))
