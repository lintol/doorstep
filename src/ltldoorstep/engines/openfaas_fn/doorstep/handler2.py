from orp_sim_firstgen.result_service import ResultService

from .concentric import ConcentricRisk

risk = ConcentricRisk

def handle(*args):
    result_service = ResultService()  # creating new ResultService object
    result = result_service.retrieve(risk(), *args)
    return result
