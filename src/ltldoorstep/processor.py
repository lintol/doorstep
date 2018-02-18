import os
import logging

class DoorstepProcessor:
    pass

class ReportItem:
    def __init__(self, typ, location, definition, properties):
        self.type = typ
        self.location = location
        self.definition = definition
        self.properties = properties

    @classmethod
    def parse(cls, data):
        return cls(
            typ=data['entity']['type'],
            location=data['entity']['location'],
            definition=data['entity']['definition'],
            properties=data['properties']
        )

    def render(self):
        return {
            'entity': {
                'type': self.type,
                'location': self.location,
                'definition': self.definition
            },
            'properties': self.properties
        }

class ReportItemLiteral(ReportItem):
    def __init__(self, literal):
        self.literal = literal

    def render(self):
        return self.literal

class ReportIssue:
    def __init__(self, level, item, context, processor, code, message, error_data={}):
        self.level = level
        self.processor = processor
        self.code = code
        self.message = message
        self.item = item
        self.context = context
        self.error_data = error_data

    def get_item(self):
        return self.item

    @classmethod
    def parse(cls, level, data):
        return cls(
            level=level,
            processor=data['processor'],
            code=data['code'],
            message=data['message'],
            item=ReportItem.parse(data['item']),
            context=[ReportItem.parse(c) for c in data['context']],
            error_data=(data['error-data'] if 'error-data' in data else {})
        )

    def render(self):
        return {
            'processor': self.processor,
            'code' : self.code,
            'message' : self.message,
            'item': self.item,
            'context': [c.parse() for c in self.context],
            'error-data': self.error_data
        }

class ReportIssueLiteral(ReportIssue):
    def __init__(self, level, literal, literal_item):
        self.level = level
        self.literal = literal
        self.item = ReportItemLiteral(literal_item)

    def render(self):
        return self.literal

class Report:
    def __init__(self, filename, metadata, headers, preset, encoding, time, row_count, supplementary, issues=None):
        if issues is None:
            self.issues = {
                logging.ERROR: [],
                logging.WARNING: [],
                logging.INFO: []
            }
        else:
            self.issues = issues

        self.supplementary = supplementary
        self.filename = filename
        self.metadata = metadata

        self.properties = {
            'row-count': row_count,
            'time': time,
            'encoding': encoding,
            'preset': preset,
            'headers': headers
        }

    def get_issues(self, level=None):
        if level:
            return self.issues[level]
        return sum(list(self.issues.values()))

    def append_issue(self, issue):
        self.issues[issue.level].append(issue)

    @classmethod
    def make(cls):
        return cls(
            filename=None,
            metadata={},
            headers=[],
            preset='tabular',
            encoding='utf-8',
            time='0.0',
            row_count=0,
            supplementary={}
        )

    @classmethod
    def parse(cls, dictionary):
        issues = {}

        table = dictionary['tables'][0]

        issues[logging.ERROR] = [
            ReportIssue.parse(logging.ERROR, issue)
            for issue in
            table['errors']
        ]

        issues[logging.WARNING] = [
            ReportIssue.parse(logging.WARNING, issue)
            for issue in
            table['warnings']
        ]

        issues[logging.INFO] = [
            ReportIssue.parse(logging.INFO, issue)
            for issue in
            table['informations']
        ]

        filename = table['filename']
        metadata = {
            'fileType': table['format']
        }
        supplementary = dictionary['supplementary']
        row_count = table['row-count']
        time = table['time']
        encoding = table['encoding']
        preset = table['preset']
        headers = table['headers']

        return cls(
            filename=filename,
            supplementary=supplementary,
            row_count=row_count,
            time=time,
            encoding=encoding,
            preset=preset,
            headers=headers,
            metadata=metadata,
            issues=issues
        )

    def compile(self):
        report = self.issues
        supplementary = self.supplementary
        metadata = self.metadata
        filename = self.filename

        if metadata and 'fileType' in metadata:
            frmt = metadata['fileType']
        else:
            root, frmt = os.path.splitext(filename)
            if frmt and frmt[0] == '.':
                frmt = frmt[1:]

        valid = not bool(report[logging.ERROR])
        return {
            'supplementary': supplementary,
            'error-count': sum([len(r) for r in report.values()]),
            'valid': valid,
            'tables': [
                {
                    'format': frmt,
                    'errors': report[logging.ERROR],
                    'warnings': report[logging.WARNING],
                    'informations': report[logging.INFO],
                    'row-count': self.properties['row-count'],
                    'headers': self.properties['headers'],
                    'source': filename,
                    'time': self.properties['time'],
                    'valid': valid,
                    'scheme': 'file',
                    'encoding': self.properties['encoding'],
                    'schema': None,
                    'error-count': sum([len(r) for r in report.values()])
                }
            ],
            'preset': self.properties['preset'],
            'warnings': [],
            'table-count': 1,
            'time': self.properties['time']
        }

    def add_supplementary(self, typ, source, name):
        self.supplementary.append({
            'type': typ,
            'source': source,
            'name': name
        })

    def set_properties(self, properties):
        for arg in self.properties:
            if arg in properties:
                properties[arg] = self.properties[arg]


# TODO: refactor into the incoming Report singleton classes
report = Report.make()

def add_supplementary(typ, source, name):
    global report

    report.add_supplementary(typ, source, name)

def set_properties(**kwargs):
    global report

    report.set_properties(kwargs)

def tabular_add_issue(processor, log_level, code, message, row_number=None, column_number=None, row=None):
    global report

    if log_level not in (logging,INFO, logging.WARNING, logging.ERROR):
        raise RuntimeError(_('Log-level must be one of logging.INFO, logging.WARNING or logging.ERROR'))

    if row_number:
        if column_number:
            typ = 'cell'
        else:
            typ = 'row'
    else:
        if column_number:
            typ = 'column'
        else:
            typ = 'global'

    entry = {
        'processor': processor,
        'code': code,
        'message': message,
        'row-number': row_number,
        'column-number': column_number,
        'row': row if row else [],
        'item': {
            'entity': {
                'type': typ,
                'location': {
                    'row': row_number,
                    'column': column_number
                },
                'definition': None
            },
            'properties': {}
        }
    }
    if row:
        entry['context'] = [{
            'item': {
                'entity': {
                    'type': typ,
                    'location': {
                        'row': row_number,
                        'column': column_number
                    },
                    'definition': None
                },
                'properties': {}
            }
        }]
    report.append_issue(ReportIssueLiteral(log_level, entry, entry))

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
    report.append_issue(ReportIssue.parse(log_level, entry))

def compile_report(filename, metadata):
    global report

    report.filename = filename
    if metadata:
        report.metadata = metadata

    return report.compile()


def properties_from_report(report):
    table = report['tables'][0]
    return {
        'row-count': table['row-count'],
        'time': report['time'],
        'encoding': table['encoding'],
        'preset': report['preset'],
        'headers': table['headers']
    }

def combine_reports(base, additional):
    for base_table, additional_table in zip(base['tables'], additional['tables']):
        for level in ('errors', 'warnings', 'informations'):
            if level in additional_table:
                if level not in base_table:
                    base_table[level] = []
                base_table[level] += additional_table[level]
        base_table['error-count'] += additional_table['error-count']

    if 'supplementary' in additional:
        if 'supplementary' not in base:
            base['supplementary'] = []
        base['supplementary'] += additional['supplementary']

    return base
