import numpy as np
from dask.threaded import get
import sys
import pandas as pd
import re
import unicodedata
import unicodeblock.blocks
import logging
from ltldoorstep.processor import DoorstepProcessor

unicode_category_major = {
    'L': ('letter'),
    'M': ('mark'),
    'N': ('number'),
    'P': ('punctuation'),
    'S': ('symbol'),
    'Z': ('separator'),
    'C': ('control character')
}


def check_character_blocks(csv):
    # All characters are Latinate (unicodedata.normalize('NFKD')[0] is Latin)
    string_csv = csv.select_dtypes(include=['object'])

    block_set = set()
    # consider double @functools.lru_cache(maxsize=128, typed=False) if required
    string_csv.apply(np.vectorize(lambda cell: block_set.update({unicodeblock.blocks.of(c) for c in cell})))

    report = {}

    if None in block_set:
        block_set.remove(None)
        report['check_character_blocks:unknown-cat'] = ('Unknown character type found', logging.WARNING, None)

    report['check_character_blocks:blocks-found'] = ('Character blocks found', logging.INFO, ', '.join(block_set))

    return report

def check_character_categories(csv):
    # All characters are Latinate (unicodedata.normalize('NFKD')[0] is Latin)
    string_csv = csv.select_dtypes(include=['object'])
    categories = set()
    # consider double @functools.lru_cache(maxsize=128, typed=False) if required
    string_csv.apply(np.vectorize(lambda cell: categories.update({unicodedata.category(c) for c in cell})))

    return {
        'check_character_categories:cat-found': ('Character categories found', logging.INFO, [unicode_category_major[c[0]] for c in categories])
    }

def check_std_dev():
    # Standard Deviation is non-negative
    names_for_standard_deviation = [
        r'std[. ]*dev',
        r'standard deviation',
    ]

    bad_rows = {}
    matches = [re.match(rex, col, re.IGNORECASE) for rex in names_for_standard_deviation for col in row.dtypes.index]
    for match in matches:
        col = match[0]
        if col not in bad_rows:
            bad_rows[col] = filter(lambda r: r[col] < 0, csv.iterrows())

    return {
        'check_std_dev:neg-std-dev': ('Rows with negative Standard Deviation', logging.WARNING, bad_rows)
    }

def check_ids_unique(csv):
    # IDs are unique
    ids = csv['ID']
    report = {}
    min_duplicates = len(set(ids)) < len(ids)
    if min_duplicates > 0:
        report['check_ids_unique:not-unique'] = ('IDs are not unique', logging.WARNING, 'At least %d duplicates' % min_duplicates)
    return report

def check_ids_surjective(csv):
    # IDs are surjective onto their range
    ids = csv['ID']
    report = {}
    unique_ids = len(set(ids))
    expected_ids = max(ids) - min(ids) + 1
    if expected_ids != unique_ids:
        report['check_ids_surjective:not-surjective'] = ('IDs are missing', logging.WARNING, '%d missing between %d and %d' % (expected_ids - unique_ids, min(ids), max(ids)))
    return report

class CsvCheckerProcessor(DoorstepProcessor):
    def get_workflow(self, filename, metadata={}):
        workflow = {
            'load-csv': (pd.read_csv, filename),
            'step-A': (check_ids_surjective, 'load-csv'),
            'step-B': (check_ids_unique, 'load-csv'),
            'step-C': (check_character_categories, 'load-csv'),
            'step-D': (check_character_blocks, 'load-csv'),
            'output': (list, ['step-A', 'step-B', 'step-C', 'step-D'])
        }
        return workflow

processor = CsvCheckerProcessor

if __name__ == "__main__":
    argv = sys.argv
    processor = CsvCheckerProcessor()
    workflow = processor.get_workflow(argv[1])
    print(get(workflow, 'output'))
