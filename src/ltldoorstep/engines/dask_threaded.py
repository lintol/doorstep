"""Engine for running a job, using dask, within this process."""

from importlib.machinery import SourceFileLoader
from ..file import make_file_manager
from .dask_common import run


class DaskThreadedEngine:
    """Allow execution of a dask workflow within this process."""

    @staticmethod
    async def run(filename, workflow_module, bucket=None):
        """Start the multi-threaded execution process."""

        mod = SourceFileLoader('custom_processor', workflow_module)

        result = None
        with make_file_manager(bucket) as file_manager:
            local_file = file_manager.get(filename)
            result = run(local_file, mod.load_module())

        return result
