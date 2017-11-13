"""Check Pachyderm interaction is correct."""

from unittest.mock import Mock, patch, mock_open
import pytest
import pypachy
from ltldoorstep.engines.pachyderm import PachydermEngine

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

    pps.list_job.return_value = jobs
    pfs.get_file.return_value = [
        b'foo', b'bar', b'baz'
    ]

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

    pps, pfs = engine.get_clients()

    definition = {}
    def set_definition(**defn):
        definition.update(defn)
    pps.create_pipeline.side_effect = set_definition

    mopen = mock_open(read_data='{}')
    time = Mock()
    with patch('ltldoorstep.engines.pachyderm.open', mopen) as _, \
            patch('ltldoorstep.engines.pachyderm.time', time):
        engine.run(filename, module)

    assert 'pipeline' in definition
    assert 'name' in definition['pipeline']

    commit_full_name = '%s/master' % definition['pipeline']['name']
    pfs.get_file.assert_called_with(commit_full_name, '/doorstep.out')
