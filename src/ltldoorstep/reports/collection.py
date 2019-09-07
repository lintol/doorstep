from .report import Report

class ReportCollection:
    def __init__(self, reports):
        self._reports = []

        for report in reports:
            if not isinstance(report, Report):
                report = Report.parse(report)
            self._reports.append(report)

    def find_by_processor(self, processor, include_subprocessors=True):
        return [rprt for rprt in self._reports if rprt.has_processor(processor, include_subprocessors=include_subprocessors)]
