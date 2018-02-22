import os
import logging
from .report import Report

class DoorstepProcessor:
    @staticmethod
    def make_report():
        return Report("(unknown processor)", "(no description provided)")

    def __init__(self):
        self._report = self.make_report()

    def compile_report(self, filename='unknown.csv', metadata=None):
        return compile_report(self._report, filename, metadata)
