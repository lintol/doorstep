import os
import logging

class DoorstepProcessor:
    pass

# TODO: refactor into the incoming Report singleton classes
report = {
    logging.WARNING: [],
    logging.INFO: [],
    logging.ERROR: []
}

properties = {
    'row-count': None,
    'time': None,
    'encoding': 'utf-8',
    'preset': 'table',
    'headers': []
}

def set_properties(**kwargs):
    global properties

    for arg in properties:
        if arg in kwargs:
            properties[arg] = kwargs[arg]

def tabular_add_issue(processor, log_level, code, message, row_number=None, column_number=None, row=None):
    global report

    if log_level not in report:
        raise RuntimeError(_('Log-level must be one of logging.INFO, logging.WARNING or logging.ERROR'))

    report[log_level].append({
        'processor': processor,
        'code': code,
        'message': message,
        'row-number': row_number,
        'column-number': column_number,
        'row': row if row else []
    })

def geojson_add_issue(processor, log_level, code, message, item_index=None, item=None, item_type=None, item_properties=None):
    global report

    entry = {
        'processor': processor,
        'code': code,
        'message': message,
        'item': None
    }
    if item:
        entry['item'] = {
            'entity': {
                'type': item_type,
                'location': {
                    'index': item_index
                },
                'definition': item
            },
            'properties': item_properties
        }
    report[log_level].append(entry)

def compile_report(filename, metadata):
    global report, properties

    if metadata and 'fileType' in metadata:
        frmt = metadata['fileType']
    else:
        root, frmt = os.path.splitext(filename)
        if frmt and frmt[0] == '.':
            frmt = frmt[1:]

    valid = bool(report[logging.ERROR])

    return {
        'error-count': sum([len(r) for r in report.values()]),
        'valid': valid,
        'tables': [
            {
                'format': frmt,
                'errors': report[logging.ERROR],
                'warnings': report[logging.WARNING],
                'informations': report[logging.INFO],
                'row-count': properties['row-count'],
                'headers': properties['headers'],
                'source': filename,
                'time': properties['time'],
                'valid': valid,
                'scheme': 'file',
                'encoding': properties['encoding'],
                'schema': None,
                'error-count': sum([len(r) for r in report.values()])
            }
        ],
        'preset': properties['preset'],
        'warnings': [],
        'table-count': 1,
        'time': properties['time']
    }
