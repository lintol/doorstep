"""Check dask distributed engine is correctly functioning."""

from unittest.mock import Mock, patch, mock_open
from distributed import utils
from distributed import utils_test
import asyncio
import pytest
from ltldoorstep.engines.dask_distributed import DaskDistributedEngine
from contextlib import contextmanager


@pytest.fixture
def engine():
    """Create a distributed dask engine."""

    eng = DaskDistributedEngine()

    return eng

def test_can_run_workflow(engine):
    """Check we can run a workflow on local files."""

    filename = '/tmp/foo.csv'
    module = '/tmp/bar.py'

    test_module = '''
def ret(filename):
    return filename.upper()

def get_workflow(filename):
    return {'output': (ret, filename)}
    '''

    mopen = mock_open(read_data=test_module)
    loop = asyncio.get_event_loop()

    with patch('distributed.client.open', mopen) as _:
        # Note that the event loop is handled within utils_test
        @utils_test.gen_cluster(client=True)
        async def _exec(c, s, a, b):
            engine.client = c
            return await engine.run(filename, module)

        result = _exec()

    assert result == filename.upper()
