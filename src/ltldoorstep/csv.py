import numpy as np
import re
import unicodedata
import unicodeblock.blocks
import logging

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
        report['Unknown character category found'] = (logging.WARNING, None)

    report['Character categories found'] = (logging.INFO, ', '.join(block_set))

    return report

def check_character_categories(csv):
    # All characters are Latinate (unicodedata.normalize('NFKD')[0] is Latin)
    string_csv = csv.select_dtypes(include=['object'])
    categories = set()
    # consider double @functools.lru_cache(maxsize=128, typed=False) if required
    string_csv.apply(np.vectorize(lambda cell: categories.update({unicodedata.category(c) for c in cell})))

    return {
        'Character categories found': (logging.INFO, {unicode_category_major[c[0]] for c in categories})
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
        'Rows with negative Standard Deviation': (logging.WARNING, bad_rows)
    }

def check_ids_unique(csv):
    # IDs are unique
    ids = csv['ID']
    report = {}
    min_duplicates = len(set(ids)) < len(ids)
    if min_duplicates > 0:
        report['IDs are not unique'] = (logging.WARNING, 'At least %d duplicates' % min_duplicates)
    return report

def check_ids_surjective(csv):
    # IDs are surjective onto their range
    ids = csv['ID']
    report = {}
    unique_ids = len(set(ids))
    expected_ids = max(ids) - min(ids) + 1
    if expected_ids != unique_ids:
        report['IDs are missing'] = (logging.WARNING, '%d missing between %d and %d' % (expected_ids - unique_ids, min(ids), max(ids)))
    return report

def get_workflow():
    return [
        check_ids_surjective,
        check_ids_unique,
        check_character_categories,
        check_character_blocks
    ]
