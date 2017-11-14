"""Engine for running a job, using dask, within this process."""

import uuid
from contextlib import contextmanager
from importlib.machinery import SourceFileLoader
from ..file import make_file_manager
from .dask_common import run


class DaskThreadedEngine:
    """Allow execution of a dask workflow within this process."""

    def add_data(self, filename, content, session):
        session['data-filename'] = filename
        session['data-content'] = content

    def add_processor(self, filename, content, session):
        session['workflow-filename'] = filename
        session['workflow-content'] = content

    async def monitor_pipeline(self, session):
        workflow_module = session['workflow-filename']
        filename = session['data-filename']
        content = {
            workflow_module: session['workflow-content'].decode('utf-8'),
            filename: session['data-content'].decode('utf-8'),
        }

        with make_file_manager(content=content) as file_manager:
            mod = SourceFileLoader('custom_processor', file_manager.get(workflow_module))
            local_file = file_manager.get(filename)
            result = run(local_file, mod.load_module())

        return result

    @staticmethod
    async def run(filename, workflow_module, bucket=None):
        """Start the multi-threaded execution process."""

        mod = SourceFileLoader('custom_processor', workflow_module)

        result = None
        with make_file_manager(bucket) as file_manager:
            local_file = file_manager.get(filename)
            result = run(local_file, mod.load_module())

        return result

    @contextmanager
    def make_session(self):
        """Set up a workflow session.

        This creates a self-contained set of dask constructs representing our operation.
        """

        name = 'doorstep-%s' % str(uuid.uuid4())
        data_name = '%s-data' % name
        processors_name = '%s-processors' % name

        session = {
            'name': name,
            'data': data_name,
            'processors': processors_name
        }

        yield session
