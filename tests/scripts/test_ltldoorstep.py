import pytest
import contextlib
import sys
import asynctest
from ltldoorstep.reports.report import TabularReport
import ltldoorstep.scripts.ltldoorstep as sc
from unittest.mock import Mock, patch
from click.testing import CliRunner


@pytest.fixture
def runner():
    return CliRunner()


@contextlib.contextmanager
def fake_ckan():
    ckan = Mock()
    mod = None
    if 'ckanapi' in sys.modules:
        mod = sys.modules['ckanapi']
    sys.modules['ckanapi'] = ckan
    yield ckan
    if mod:
        sys.modules['ckanapi'] = mod


def test_crawl(runner):
    requests = Mock()
    file_manager = Mock()
    sc.engines['test'] = Mock()
    run = asynctest.CoroutineMock(return_value=TabularReport('foo', 'bar').compile())
    sc.engines['test'].return_value = Mock()
    sc.engines['test'].return_value.run = run

    @contextlib.contextmanager
    def make_file_manager(content):
        yield file_manager

    with fake_ckan() as ckan, \
            patch('ltldoorstep.scripts.ltldoorstep.requests', requests), \
            patch('ltldoorstep.scripts.ltldoorstep.make_file_manager', make_file_manager):
        client = Mock()
        client.action.resource_search.return_value = {
            'results': [{'url': 'https://ckan.example.net/test'}]
        }
        ckan.RemoteCKAN.return_value = client
        result = runner.invoke(sc.cli, [
            '--output', 'json',
            '--debug', 'crawl',
            'test.py',
            '--url', 'https://ckan.example.com',
            '--engine', 'test'
        ])

    print(result.output)
    assert result.exit_code == 0


def test_process(runner):
    sc.engines['test'] = Mock()
    run = asynctest.CoroutineMock(return_value=TabularReport('foo', 'bar').compile())
    sc.engines['test'].return_value = Mock()
    sc.engines['test'].return_value.run = run

    result = runner.invoke(sc.cli, [
        '--output', 'json',
        '--debug',
        'process',
        'test.csv',
        'test.py',
        '--engine', 'test'
    ])

    assert result.exit_code == 0


def test_engine_info(runner):
    requests = Mock()
    file_manager = Mock()
    sc.engines['test'] = Mock()
    sc.engines['test'].description.return_value = 'foo'
    sc.engines['test'].config_help.return_value = {'bar': 'baz'}

    result = runner.invoke(sc.cli, [
        '--output', 'json',
        '--debug',
        'engine-info',
        'test'
    ])

    assert result.exit_code == 0
