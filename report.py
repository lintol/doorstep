import logging
"""This is the report object that will be used for the standardisation of processors reporting
The Report superclass can be inherited for different forms of reporting i.e. tabluar, GeoJSON etc."""
class Report(object):
	
	def __init__(self, processor, info, logging, location):
            self.processor = processor
            self.info = info
            self.logging = logging
            self.location = location

    def add_issue(self, name, processor, info, logging, location):
        issue(
            processor,
            logging.WARNING,
            info,
            _("test") + ': ' + str(mismatching_columns),
            error_data={'mismatching-columns': mismatching_columns}
        )




class TabularReport(Report):

	
    def __init__(self):
        super().__init__(self.processor, self.info, self.logging, self.location)
        super().add_issue(name, processor, info, logging.WARNING, location)


class GeoJSONReport(Report):
    def __init__(self):
        super().__init__(self.processor, self.info, self.logging, self.location)
        super().add_issue(name, processor, info, logging.WARNING, location)





		
