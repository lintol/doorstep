"""Check Pachyderm interaction is correct."""

from unittest.mock import Mock, patch, mock_open
import asyncio
import pytest
import pypachy
from ltldoorstep.engines.pachyderm import PachydermEngine
from ltldoorstep.engines.pachyderm_proxy.job import PachydermJob
from concurrent.futures import ThreadPoolExecutor

@pytest.fixture
def job():
    """Get a mock pypachy job."""

    job = Mock()
    job.state = pypachy.JOB_SUCCESS

    return job


@pytest.fixture
def engine(job):
    """Create an engine with mock Pachyderm clients."""

    pfs = Mock()
    pps = Mock()

    jobs = Mock()
    jobs.job_info = [job]

    PachydermJob._execute = asyncio.coroutine(lambda *args: jobs)
    futs = (asyncio.Future(), asyncio.Future())
    for fut in futs:
        fut.set_result(True)
    PachydermEngine.monitor_pipeline = asyncio.coroutine(lambda *args: futs)

    pps.list_job.return_value = jobs
    pfs.get_file.return_value = [
        b'foo', b'bar', b'baz'
    ]
    pfs.refreshing_subscribe_commit.return_value = []

    eng = PachydermEngine()
    eng.set_clients(
        pps,
        pfs
    )
    eng.get_definition()

    return eng

def test_can_run_local(engine):
    """Check we can run a workflow on local files."""

    filename = '/tmp/foo.csv'
    module = '/tmp/bar.py'
    metadata = {}

    pps, pfs = engine.get_clients()

    definition = {}
    def set_definition(**defn):
        definition.update(defn)
    pps.create_pipeline.side_effect = set_definition

    pfs.refreshing_subscribe_commit.return_value = []

    mopen = mock_open(read_data='{}')
    time = Mock()
    loop = asyncio.get_event_loop()

    with patch('ltldoorstep.engines.pachyderm.open', mopen) as _, \
            patch('ltldoorstep.engines.pachyderm.time', time):
        loop.run_until_complete(engine.run(filename, module, metadata))

    assert 'pipeline' in definition
    assert 'name' in definition['pipeline']

    commit_full_name = '%s/master' % definition['pipeline']['name']
    pfs.get_file.assert_called_with(commit_full_name, '/raw/processor.json')
