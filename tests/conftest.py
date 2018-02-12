import pytest
import gettext

@pytest.fixture(scope = "session", autouse = True)
def install_gettext():
    gettext.install('ltldoorstep')