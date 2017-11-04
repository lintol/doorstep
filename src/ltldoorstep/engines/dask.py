from functools import reduce
from dask import threaded
from dask.distributed import Client
from importlib.machinery import SourceFileLoader
from ..file import make_file_manager
import os
import sys

class DaskThreadedProcessor:
    def run(self, filename, workflow_module, bucket=None):
        mod = SourceFileLoader('custom_processor', workflow_module)

        result = None
        with make_file_manager(bucket) as file_manager:
            local_file = file_manager.get(filename)
            result = _run(local_file, mod.load_module())

        return result

class DaskDistributedProcessor:
    def run(self, filename, workflow_module, bucket=None):
        result = None

        with make_file_manager(bucket) as file_manager:
            local_file = file_manager.get(filename)

            client = Client('tcp://localhost:8786')
            client.upload_file(workflow_module)
            module_name = os.path.splitext(os.path.basename(workflow_module))[0]
            result = client.submit(_execute, local_file, module_name).result()

        return result

def _execute(filename, module_name):
    mod = __import__(module_name)
    return _run(filename, mod)

def _run(filename, mod):
    result = {}
    workflow = mod.get_workflow(filename)
    return threaded.get(workflow, 'output')
