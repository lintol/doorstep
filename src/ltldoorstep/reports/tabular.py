from .report import Report, ReportItem
import itertools

class TabularReport(Report):

    preset = 'tabular'

    def add_issue(self, log_level, code, message, row_number=None, column_number=None, row=None, error_data=None):
        """This function will add an issue to the report and takes as parameters the processor, the log level, code, message"""

        if row_number:
            if column_number:
                typ = 'Cell'
            else:
                typ = 'Row'
        else:
            if column_number:
                typ = 'Column'
            else:
                typ = 'Global'

        if row and self.properties['headers']:
            row_pairs = itertools.zip_longest(self.properties['headers'], row)
            row = {k: v for k, v in row_pairs if k is not None}

        item = ReportItem(typ, {'row': row_number, 'column': column_number}, None, row)

        super(TabularReport, self).add_issue(log_level, code, message, item, error_data=error_data)
