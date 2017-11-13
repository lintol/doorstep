"""Check dask threaded engine is correctly functioning."""

from unittest.mock import Mock, patch, mock_open
import pytest
import asyncio
from ltldoorstep.engines.dask_threaded import DaskThreadedEngine

@pytest.fixture
def engine():
    """Create a threaded dask engine."""

    eng = DaskThreadedEngine()

    return eng

def test_can_run_workflow(engine):
    """Check we can run a workflow on local files."""

    filename = '/tmp/foo.csv'
    module = '/tmp/bar.py'

    mopen = mock_open(read_data='{}')
    source_file_loader_path = 'ltldoorstep.engines.dask_threaded.SourceFileLoader'
    source_file_loader = Mock()

    module = Mock()
    processor = Mock()

    source_file_loader().load_module.return_value = module
    module.get_workflow.return_value = {
        'output': (processor, 'foo')
    }
    loop = asyncio.get_event_loop()

    with patch('ltldoorstep.engines.dask_threaded.open', mopen) as _, \
            patch(source_file_loader_path, source_file_loader):
        loop.run_until_complete(engine.run(filename, module))

    processor.assert_called_with('foo')
