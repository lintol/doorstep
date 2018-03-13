import os
from dask.threaded import get
from unittest.mock import Mock
import pytest
import sys
import subprocess
import asynctest
import socket

@pytest.fixture
def nltk():
    nltk = Mock()
    sys.modules['nltk.tag.stanford'] = nltk
    sys.modules['nltk.tokenize.treebank'] = nltk
    sys.modules['nltk.tokenize'] = nltk
    os.environ['STANFORD_MODELS'] = '/tmp'

    return nltk

@pytest.fixture
def PiiProcessor(nltk):
    from ltldoorstep_examples.pii import PiiProcessor
    return PiiProcessor

def test_pii_checker_on_pii(nltk, PiiProcessor, monkeypatch):
    path = os.path.join(os.path.dirname(__file__), 'data', 'pii.csv')

    ner = Mock()
    nltk.CoreNLPNERTagger.return_value = ner
    ner.tag_sents.return_value = [[('I', 'one'), ('Z', 'another')]]

    mock_socket = Mock(return_value=Mock())
    monkeypatch.setattr(socket, 'socket', mock_socket)

    mock_process = asynctest.Mock(return_value=True)
    mock_popen = asynctest.Mock(return_value=mock_process)
    monkeypatch.setattr(subprocess, 'Popen', mock_popen)

    pii_checker = PiiProcessor()
    workflow = pii_checker.get_workflow(path)
    results = get(workflow, 'output').compile()

    errors = results["tables"][0]["informations"]
    assert len(errors) == 4

    report = errors[0]
    assert report['item']['entity']['location'] == {'row': 0, 'column': 2}

