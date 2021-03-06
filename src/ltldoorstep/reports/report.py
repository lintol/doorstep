"""This is the report object that will be used for the standardisation of processors reporting
The Report superclass can be inherited for different forms of reporting i.e. tabular, GeoJSON etc."""


import logging
import json
import os
from ..metadata import DoorstepContext
from ..encoders import Serializable

def get_report_class_from_preset(preset):
    if preset not in _report_class_from_preset:
        raise NotImplementedError(_(
            "This version of doorstep does not have the requested Report preset type"
        ))
    return _report_class_from_preset[preset]

class ReportItem:
    def __init__(self, typ, location, definition, properties):
        self.type = typ
        self.location = location
        self.definition = definition
        self.properties = dict(properties) if properties else None

    def __str__(self):
        return json.dumps(self.render())

    def __repr__(self):
        return '(|Item: %s|)' % str(self)

    @classmethod
    def parse(cls, data):
        if data['entity']:
            return cls(
                typ=data['entity']['type'] if data['entity'] else None,
                location=data['entity']['location'] if data['entity'] else None,
                definition=data['entity']['definition'] if data['entity'] else None,
                properties=data['properties']
            )
        else:
            return cls(properties=data['properties'])

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
    def __init__(self, level, item, context, processor, code, message, error_data=None):
        self.level = level
        self.processor = processor
        self.code = code
        self.message = message
        self.item = item
        self.context = context

        if error_data is None:
            error_data = {}
        self.error_data = error_data

    def __str__(self):
        return json.dumps(self.render())

    def __repr__(self):
        return '(|Issue: %s|)' % str(self)

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
            context=[ReportItem.parse(c) for c in data['context']] if data['context'] else None,
            error_data=(data['error-data'] if 'error-data' in data else {})
        )

    def render(self):
        return {
            'processor': self.processor,
            'code' : self.code,
            'message' : self.message,
            'item': self.item.render(),
            'context': [c.render() for c in self.context] if self.context else None,
            'error-data': self.error_data
        }

class ReportIssueLiteral(ReportIssue):
    def __init__(self, level, literal, literal_item):
        self.level = level
        self.literal = literal
        self.item = ReportItemLiteral(literal_item)

    def render(self):
        return self.literal

class Report(Serializable):
    preset = None

    @classmethod
    def get_preset(cls):
        if not cls.preset:
            raise NotImplementedError(_("This report class must have a preset type"))

        return cls.preset

    def has_processor(self, processor, include_subprocessors):
        if include_subprocessors:
            group = [self.processor] + self.get_subprocessors()
        else:
            group = [self.processor]

        if ':' in processor:
            comp = lambda p: processor == p
        else:
            comp = lambda p: p.split(':')[0] == processor

        return any([comp(p) for p in group])

    def get_subprocessors(self):
        subprocessors = set()
        for level in self.issues.values():
            for issue in level:
                subprocessors.add(issue.processor)
        return list(subprocessors)

    def __serialize__(self):
        return self.compile()

    def __str__(self):
        return json.dumps(self.__serialize__())

    def __repr__(self):
        return '(|Report: %s|)' % str(self)

    def __init__(self, processor, info, filename='', metadata=None, headers=None, encoding='utf-8', time=0., row_count=None, supplementary=None, issues=None):
        if issues is None:
            self.issues = {
                logging.ERROR: [],
                logging.WARNING: [],
                logging.INFO: []
            }
        else:
            self.issues = issues

        if metadata is None:
            metadata = {}
        if supplementary is None:
            supplementary = []

        self.processor = processor
        self.info = info
        self.supplementary = supplementary
        self.filename = filename
        self.metadata = metadata

        self.properties = {
            'row-count': row_count,
            'time': time,
            'encoding': encoding,
            'preset': self.get_preset(),
            'headers': headers
        }

    def get_issues(self, level=None):
        if level:
            return self.issues[level]
        return sum(list(self.issues.values()), [])

    def get_issues_by_code(self, code, level=None):
        issues = self.get_issues(level)
        return [issue for issue in issues if issue.code == code]

    def append_issue(self, issue, prepend=False):
        if prepend:
            self.issues[issue.level].insert(0, issue)
        else:
            self.issues[issue.level].append(issue)

    @classmethod
    def load(cls, file_obj):
        return cls.parse(json.load(file_obj))

    @classmethod
    def parse(cls, dictionary):
        issues = {
            logging.INFO: [],
            logging.WARNING: [],
            logging.ERROR: []
        }
        # table = {}
        filename = dictionary['filename']

        row_count = None
        time = None
        encoding = None
        headers = None

        metadata = None
        if 'metadata' in dictionary:
            if isinstance(dictionary['metadata'], DoorstepContext):
                metadata = dictionary['metadata']
            else:
                metadata = DoorstepContext.from_dict(dictionary['metadata'])

        for table in dictionary['tables']:
            # print("*****type check**** %s" % type(dictionary))

            issues[logging.ERROR] += [
                ReportIssue.parse(logging.ERROR, issue)
                for issue in
                    table['errors']
            ]

            issues[logging.WARNING] += [
                ReportIssue.parse(logging.WARNING, issue)
                for issue in
                    table['warnings']
            ]

            issues[logging.INFO] += [
                ReportIssue.parse(logging.INFO, issue)
                for issue in
                    table['informations']
            ]

            if metadata is None:
                metadata = DoorstepContext(context_format=table['format'])

            row_count = table['row-count'] if 'time' in table else None
            time = table['time'] if 'time' in table else None
            encoding = table['encoding'] if 'encoding' in table else None
            headers = table['headers'] if 'headers' in table else None

        supplementary = dictionary['supplementary']
        cls = get_report_class_from_preset(dictionary['preset'])

        return cls(
            '(unknown)',
            '(parsed by ltldoorstep)',
            filename=filename,
            supplementary=supplementary,
            row_count=row_count,
            time=time,
            encoding=encoding,
            headers=headers,
            metadata=metadata,
            issues=issues
        )

    def update(self, additional):
        for level, issues in additional.issues.items():
            self.issues[level] += issues

        self.supplementary += additional.supplementary

        for prop in ('row-count', 'encoding', 'headers', 'preset'):
            if self.properties[prop] is None:
                self.properties[prop] = additional.properties[prop]

    @staticmethod
    def table_string_from_issue(issue):
        return ''

    def compile(self, filename=None, metadata=None):
        supplementary = self.supplementary

        if filename is None:
            filename = self.filename

        if not filename:
            filename = 'unknown.csv'

        if metadata is None:
            metadata = self.metadata

        if metadata and metadata.context_format:
            frmt = metadata.context_format
        else:
            root, frmt = os.path.splitext(filename)
            if frmt and frmt[0] == '.':
                frmt = frmt[1:]

        table_strings = {self.table_string_from_issue(issue) for issues in self.issues.values() for issue in issues}
        table_strings = sorted(list(table_strings))
        issues_by_table = {ts: {logging.ERROR: [], logging.WARNING: [], logging.INFO: []} for ts in table_strings}
        for level, issue_list in self.issues.items():
            for issue in issue_list:
                issues_by_table[self.table_string_from_issue(issue)][level].append(issue)

        tables = []
        total_errors = 0
        total_valid = True
        for table_string in table_strings:
            table = issues_by_table[table_string]
            report = {k: [item.render() for item in v] for k, v in table.items()}
            error_count = sum([len(r) for r in report.values()])
            total_errors += error_count
            valid = not bool(report[logging.ERROR])
            total_valid = valid and total_valid

            tables.append(
                {
                    'format': frmt,
                    'errors': report[logging.ERROR],
                    'warnings': report[logging.WARNING],
                    'informations': report[logging.INFO],
                    'row-count': self.properties['row-count'],
                    'headers': self.properties['headers'],
                    'source': f'{filename}{table_string}',
                    'time': self.properties['time'],
                    'valid': valid,
                    'scheme': 'file',
                    'encoding': self.properties['encoding'],
                    'schema': None,
                    'error-count': error_count
                }
            )

        results = {
            'supplementary': supplementary,
            'error-count': total_errors,
            'valid': total_valid,
            'tables': tables,
            'filename': filename,
            'preset': self.get_preset(),
            'warnings': [],
            'table-count': 1,
            'time': self.properties['time']
        }

        return results

    def add_supplementary(self, typ, source, name):
        logging.warning('Adding supplementary')
        logging.warning((typ, source, name))
        self.supplementary.append({
            'type': typ,
            'source': source,
            'name': name
        })

    def set_properties(self, **properties):
        for arg in self.properties:
            if arg in properties:
                self.properties[arg] = properties[arg]


    def add_issue(self, log_level, code, message, item=None, error_data=None, context=None, at_top=False):
        """This function will add an issue to the report and takes as parameters the processor, the log level, code, message"""


        if log_level not in self.issues:
            raise RuntimeError(_('Log-level must be one of logging.INFO, logging.WARNING or logging.ERROR'))

        if not isinstance(item, ReportItem):
            item = ReportItemLiteral(item)

        issue = ReportIssue(log_level, item=item, context=context, processor=self.processor, code=code, message=message, error_data=error_data)

        self.append_issue(issue, prepend=at_top)

# TODO: fix for multiple tables
def properties_from_report(report):
    table = report['tables'][0]
    return {
        'row-count': table['row-count'],
        'time': report['time'],
        'encoding': table['encoding'],
        'preset': report['preset'],
        'headers': table['headers']
    }

def combine_reports(*reports, base=None):
    if base is None:
        presets = {report.preset for report in reports if report.preset}

        if len(presets) != 1:
            raise RuntimeError(
                _("Report combining can only be performed on reports with the same 'preset' property")
            )
        preset = list(presets)[0]
        rcls = get_report_class_from_preset(preset)
        base = rcls(None, None)

    for report in reports:
        base.update(report)

    return base


from .geojson import GeoJSONReport
from .tabular import TabularReport

_report_class_from_preset = {
    cls.get_preset(): cls for cls in
    (GeoJSONReport, TabularReport)
}
