"""Testing for report functionality"""
import pytest
from ltldoorstep.reports import report
import logging


def test_create_report():
    """Test for creating report"""
    assert report.Report("test", "test")

def test_add_issue():
    """testing to see if issues are being added properly"""
    r = report.Report("test", "test")
    r.add_issue("test", logging.WARNING, "Testing", "test message", "test te")
    assert r.issues[logging.WARNING]

def test_return_item():
    """testing if processor key in issues list is being set properly"""
    r = report.Report("test", "test")
    r.add_issue("test", logging.WARNING, "Testing", "test message", "test te")
    assert len(r.issues[logging.WARNING]) == 1
    issue = r.issues[logging.WARNING][0]
    assert issue["processor"] == "test"

def test_check_message():
    """testing if message key in issues list is being set properly"""
    r = report.Report("test", "test")
    r.add_issue("test", logging.INFO, "testtestest", "test msg 2", "test code 2")
    issue = r.issues[logging.INFO][0]
    assert issue["message"] == "test msg 2"

def test_check_code():
    """testing to see if code key is issues list is  being set properly"""
    r = report.Report("test", "test")
    r.add_issue("test", logging.WARNING, "test code 3", "test msg 3", "test")
    issue = r.issues[logging.WARNING][0]
    assert issue["code"] == "test code 3"

def assert_issues_dict():
    """testing to see if issues is 1. not empty and 2. is being set properly as a dictonary object"""
    r = report.Report("test", "test")
    r.add_issue("test", logging.WARNING, "test code 4", "test msg 4", "test")
    issue = r.issues[logging.WARNING][0]
    assert issue is not None
    assert type(issue) is dict
    assert len(issue) == 1



