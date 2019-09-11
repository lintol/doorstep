from .report import Report

class ReportCollection:
    def __init__(self, reports):
        self._reports = {}

        if not hasattr(reports, 'items'):
            reports = {k: v for k, v in enumerate(reports)}

        for idx, report in reports.items():
            if not isinstance(report, Report):
                report = Report.parse(report)
            self._reports[idx] = report

    def find_by_processor(self, processor, include_subprocessors=True):
        return {idx: rprt for idx, rprt in self._reports.items() if rprt.has_processor(processor, include_subprocessors=include_subprocessors)}
