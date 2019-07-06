"""Testing for metadata functionality"""
import pytest
from ltldoorstep.metadata import DoorstepContext
import logging


def test_can_create_metadata():
    """testing to see if we can create metadata objects"""

    metadata = DoorstepContext("image", "revision", "{test: 3}")
    # print(metadata.docker['image'])
    # assert metadata.docker["image"] == "image"
    assert metadata.docker["image"] == "{test: 3}"
    # assert metadata.docker["revision"] == "revision"
    assert metadata.docker["revision"] is None
    # assert metadata.package == {"test": 3}
    assert metadata.package is None

def test_can_convert_metadata_to_dict():
    """testing to see if we can create metadata objects"""
    metadata = DoorstepContext("image", "revision", "{test: 3}")
    assert metadata.to_dict() == {
        "docker": {
            "image": "image",
            "revision": "revision"
        },
        "context": {
            "package": {"test": 3}
        }
    }

def test_can_set_package():
    """testing to see if setting package works"""
    metadata = DoorstepContext("image", "revision")
    metadata.package = "{\"test\": 3}"
    assert metadata.package == {"test": 3}
