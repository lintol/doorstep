import logging
"""This is the report object that will be used for the standardisation of processors reporting
The Report superclass can be inherited for different forms of reporting i.e. tabluar, GeoJSON etc."""


class Report(object):


    def __init__(self, processor, info):
        self.processor = processor
        self.info = info
        self.issues = {
            logging.WARNING: [],
            logging.INFO: [],
            logging.ERROR: []
        }

    def get_issues(self):
        return self.issues

    def add_issue(self, processor, log_level, code, message, item = None):
        """This function will add an issue to the report and takes as parameters the processor, the log level, code, message"""


        if log_level not in self.issues:
            raise RuntimeError(_('Log-level must be one of logging.INFO, logging.WARNING or logging.ERROR'))

        issue_list = self.issues[log_level]

        issue_list.append({
            'processor': processor,
            'code': code,
            'message': message,
            'item': item
        })


class TabularReport(Report):

    def add_issue(self, processor, log_level, code, message, row_number=None, column_number=None, row=None):
        """This function will add an issue to the report and takes as parameters the processor, the log level, code, message"""

        item = {
            'entity': {
                'type': item_type,
                'location': {
                    'row': row_number,
                    'column': column_number,
                },
                'definition': item
            },
            'properties': item_properties
        }

        super().add_issue(processor, log_level, code, message, item)


class GeoJSONReport(Report):

    def add_issue(self, processor, log_level, code, message, item_index=None, item=None, item_type=None,
                          item_properties=None):
        """This function will add an issue to the report and takes as parameters the processor, the log level, code, message"""

        if item:
            item = {
                'entity': {
                    'type': item_type,
                    'location': {
                        'index': item_index
                    },
                    'definition': item
                },
                'properties': item_properties
            }

        super().add_issue(processor, log_level, code, message, item)
