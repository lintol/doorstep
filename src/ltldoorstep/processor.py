import os
import logging
from .reports.report import Report, get_report_class_from_preset

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
