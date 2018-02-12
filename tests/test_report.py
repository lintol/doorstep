"""Testing for report functionality"""
import pytest
from ltldoorstep import report
import logging


r = report.Report("test", "test")

def test_create_report():
    """Test for creating report"""
    assert report.Report("test", "test")

def test_add_issue():
    r.add_issue("test", logging.WARNING,"TEST","test message")
    assert r.issues[logging.WARNING]

