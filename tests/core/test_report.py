"""Testing for report functionality"""
import pytest
from ltldoorstep.reports import report
from ltldoorstep.reports.tabular import TabularReport
from ltldoorstep.reports.geojson import GeoJSONReport
import logging


rcls = (TabularReport, GeoJSONReport)


@pytest.mark.parametrize('rcls', rcls)
def test_create_report(rcls):
    """Test for creating report"""
    assert rcls("test", "test")

@pytest.mark.parametrize('rcls', rcls)
def test_add_issue(rcls):
    """testing to see if issues are being added properly"""
    r = rcls("test", "test")
    r.add_issue(logging.WARNING, "Testing", "test message", "test te")
    assert r.issues[logging.WARNING]

@pytest.mark.parametrize('rcls', rcls)
def test_return_item(rcls):
    """testing if processor key in issues list is being set properly"""
    r = rcls("test", "test")
    r.add_issue(logging.WARNING, "Testing", "test message", "test te")
    assert len(r.issues[logging.WARNING]) == 1
    issue = r.issues[logging.WARNING][0]
    assert issue.processor == "test"

@pytest.mark.parametrize('rcls', rcls)
def test_check_message(rcls):
    """testing if message key in issues list is being set properly"""
    r = rcls("test", "test")
    r.add_issue(logging.INFO, "testtestest", "test msg 2", "test code 2")
    issue = r.issues[logging.INFO][0]
    assert issue.message == "test msg 2"

@pytest.mark.parametrize('rcls', rcls)
def test_check_code(rcls):
    """testing to see if code key is issues list is  being set properly"""
    r = rcls("test", "test")
    r.add_issue(logging.WARNING, "test code 3", "test msg 3", "test")
    issue = r.issues[logging.WARNING][0]
    assert issue.code == "test code 3"

@pytest.mark.parametrize('rcls', rcls)
def assert_issues_dict(rcls):
    """testing to see if issues is 1. not empty and 2. is being set properly as a dictonary object"""
    r = rcls("test", "test")
    r.add_issue(logging.WARNING, "test code 4", "test msg 4", "test")
    issue = r.issues[logging.WARNING][0]
    assert issue is not None
    assert type(issue) is dict
    assert len(issue) == 1
