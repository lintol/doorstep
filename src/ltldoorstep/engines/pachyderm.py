from functools import reduce
from dask import threaded
from dask.distributed import Client
import os
import pypachy

class PachydermProcessor:
    def run(self, filename, workflow_module):
        client = pypachy.pps_client.PpsClient()

        # Turn in a context (although this may be a niche use)
        client.upload_file(workflow_module)
        module_name = os.path.splitext(os.path.basename(workflow_module))[0]
        result = client.submit(_execute, filename, module_name).result()

        return result

def _execute(filename, module_name):
    workflow_module = __import__(module_name)

    result = {}
    workflow = workflow_module.get_workflow(filename)
    return threaded.get(workflow, 'output')
