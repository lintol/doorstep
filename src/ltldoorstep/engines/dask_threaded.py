"""Engine for running a job, using dask, within this process."""

import uuid
from contextlib import contextmanager
from importlib.machinery import SourceFileLoader
from ..file import make_file_manager
from .dask_common import run
from .engine import Engine


class DaskThreadedEngine(Engine):
    """Allow execution of a dask workflow within this process."""

    def add_data(self, filename, content, redirect, session):
        session['data-filename'] = filename
        session['data-content'] = content

    def add_processor(self, filename, content, metadata, session):
        session['workflow-filename'] = filename
        session['workflow-content'] = content
        session['metadata'] = metadata

    async def monitor_pipeline(self, session):
        workflow_module = session['workflow-filename']
        metadata = session['metadata']
        filename = session['data-filename']
        content = {
            workflow_module: session['workflow-content'].decode('utf-8'),
            filename: session['data-content'].decode('utf-8'),
        }

        with make_file_manager(content=content) as file_manager:
            mod = SourceFileLoader('custom_processor', file_manager.get(workflow_module))
            local_file = file_manager.get(filename)
            result = run(local_file, mod.load_module(), metadata)

        return result

    @staticmethod
    async def run(filename, workflow_module, metadata, bucket=None):
        """Start the multi-threaded execution process."""

        mod = SourceFileLoader('custom_processor', workflow_module)

        result = None
        with make_file_manager(bucket) as file_manager:
            local_file = file_manager.get(filename)
            result = run(local_file, mod.load_module(), metadata)

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
