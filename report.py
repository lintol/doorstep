import logging
"""This is the report object that will be used for the standardisation of processors reporting
The Report superclass can be inherited for different forms of reporting i.e. tabluar, GeoJSON etc."""


class Report(object):

    def __init__(self, processor, info, logging):
        self.processor = processor
        self.info = info
        self.logging = logging.NONE

    @staticmethod
    def add_issue(name, info, logging):
        return issue(
            name,
            logging.WARNING,
            info,
            _("test") + ': ' + str(mismatching_columns),
            error_data={'mismatching-columns': mismatching_columns}
        )


class TabularReport(Report):
    @staticmethod
    def add_issue(name, info, logging):
        return issue(
            name,
            logging.WARNING,
            info,
            _("test") + ': ' + str(mismatching_columns),
            error_data={'mismatching-columns': mismatching_columns}
        )



class GeoJSONReport(Report):
    @staticmethod
    def add_issue(name, info, logging):
        return issue(
            name,
            logging.WARNING,
            info,
            _("test") + ': ' + str(mismatching_columns),
            error_data={'mismatching-columns': mismatching_columns}
        )
