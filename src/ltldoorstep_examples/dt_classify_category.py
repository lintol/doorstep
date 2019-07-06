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
from dask.threaded import get
import requests

import json

METADATA_ROWS = {
    'name': (10, lambda x: [x['name']] if 'name' in x else []),
    'notes': (3, lambda x: [x['notes']] if 'notes' in x else []),
    'resource name': (3, lambda x: [r['name'] for r in x['resources'] if 'name' in r] if 'resources' in x else []),
    'resource description': (5, lambda x: [r['description'] for r in x['resources']] if 'resources' in x else []),
    'group title': (10, lambda x: [r['title'] for r in x['groups']] if 'groups' in x else []),
    'tags': (10, lambda x: [r['name'] for r in x['tags']] if 'tags' in x else []),
    'topicCategories': (10, lambda x: x['topic_category'] if 'topic_category' in x else [])
}

def get_sentences_from_metadata(context):
    data_lines = []
    pkg_metadata = context.package

    for k, (weight, extractor) in METADATA_ROWS.items():
        extracted = extractor(pkg_metadata)
        w = weight / len(extracted)
        data_lines += [(k, v, w) for v in extracted]

    return data_lines

def get_categories(sentences, context):
    category_server = context.get_setting('categoryServerUrl', 'http://localhost:8000/')
    result = requests.post(category_server, json={'sentences': sentences})

    if result.status_code == 400:
        raise RuntimeError(_("Malformed category request: ") + result.content.decode('utf-8'))
    elif result.status_code != 200:
        raise RuntimeError(_("Could not access category service"))

    return result.json()

def get_sentences_from_data(csv):
    return []

def classify_sentences(rprt, data_sentences, metadata_sentences, context):
    sentences = data_sentences + metadata_sentences

    keys, sentences, weights = zip(*sentences)
    results = get_categories(sentences, context)

    all_categories = {}
    for key, lines, result, weight in zip(keys, sentences, results, weights):
        result = sorted(result, key=lambda r: r[0], reverse=True)
        categories = {tuple(c): weight * r * 100 for r, c in result if r > 0.2}
        issue_text = _("Possible categories in {}: {}").format(key, ", ".join(["{} ({:.2f}%)".format(_(c), r * 100) for r, c in result if r > 0.2]))
        slug_key = key.lower().replace(' ', '-')

        for c, w in categories.items():
            if c in all_categories:
                all_categories[c] += w
            else:
                all_categories[c] = w

        if result:
            rprt.add_issue(
                logging.INFO,
                'possible-categories-{}'.format(slug_key),
                issue_text,
                row_number='Metadata',
                row={key: lines},
                error_data=result,
                at_top=True
            )
    if all_categories:
        result = [('{}:{}'.format(_(c[0]), _(c[1])), w) for c, w in sorted(all_categories.items(), key=lambda r: r[1], reverse=True)]
        issue_text = _("Possible categories (all, weighted): {}").format(", ".join(["{} ({:.2f}%)".format(c, w) for c, w in result]))
        rprt.add_issue(
            logging.INFO,
            'possible-categories-all-weighted',
            issue_text,
            error_data=result,
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

    def get_workflow(self, filename, metadata):
        # setting up workflow dict
        workflow = {
            'load_csv': (p.read_csv, filename),
            'sentences_from_data': (get_sentences_from_data, 'load_csv'),
            'sentences_from_metadata': (get_sentences_from_metadata, self.metadata),
            'output': (classify_sentences, self._report, 'sentences_from_data', 'sentences_from_metadata', self.metadata)
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
