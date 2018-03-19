"""Functionality that should be available from all engines."""

import pytest
import asyncio
from ltldoorstep.engines import engines

@pytest.mark.parametrize('engine', engines.values())
def test_can_instantiate(engine):
    """Check whether engine can be created with a run method."""

    eng = engine()

    assert asyncio.iscoroutinefunction(eng.run)
    assert (engine.config_help() is None or isinstance(engine.config_help(), dict))
    assert isinstance(engine.description(), str)
