import os
import logging
from .reports.report import Report, get_report_class_from_preset, combine_reports

class DoorstepProcessor:
    preset = None
    code = None
    description = None

    @classmethod
    def make_report(cls):
        report = get_report_class_from_preset(cls.preset)

        if cls.code:
            code = cls.code
        else:
            code = _("(unknown processor)")

        if cls.description:
            description = cls.description
        else:
            description = _("(no processor description provided)")

        return report(code, description)

    def initialize(self, report=None):
        if report is None:
            report = self.make_report()
        self._report = report

    @classmethod
    def make(cls):
        new = cls()
        new.initialize()
        return new

    def compile_report(self, filename=None, metadata=None):
        return self._report.compile(filename, metadata)

    def get_report(self):
        return self._report

    def set_report(self, report):
        self._report = report
