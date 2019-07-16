from ltldoorstep.watch import Monitor
from ltldoorstep.ini import DoorstepIni
from ltldoorstep.data_store import DummyDataStore
from ltldoorstep.reports.tabular import TabularReport
import pytest


class FakePrinter:
    '''
    Printer class to create a testing object
    '''


    def build_report(self, result):
        return TabularReport("test", "test")


class FakeObject():
    pass


def test_resource_not_found():
    pass


def test_package_search(dummy_data_store):
    settings = {}
    settings['start'] = 0
    result = dummy_data_store.package_search(**settings)
    # ^ syntax? name of arg & value = overriding default values
    # args with values assigned are optional
    assert type(result['results']) is list


def test_empty_list_checked_packages():
    assert not list_checked_packages()


def list_checked_packages():
    list_checked_packages = []  # dummy version of the checked package list
    return list_checked_packages

@pytest.fixture
def monitor(printer, dummy_data_object):
    def test_gather(dummy_data_store, watch_changed_packages, settings): return None
    async def announce_fn(cmpt, resource, ini, source): return None

    monitor = Monitor(None, dummy_data_store, printer, test_gather, announce_fn)
    return monitor


@pytest.fixture
def dummy_data_store():
    client = DummyDataStore()
    return client


@pytest.fixture
def router():
    router_obj = FakeObject()
    router_obj.router_url = ['router_url']
    return router_obj.router_url


@pytest.fixture
def printer():
    return FakePrinter()


@pytest.fixture
def package_info():
    package_info = { "resources":
        [
            {
                "resource_id": "1",
                "name": "Name",
                "url": "https://url.com"
            }
        ]
    }
    return package_info


@pytest.mark.asyncio
async def test_watch_changed_packages(package_info, printer, router, monitor):
    recently_changed = [
        {
            "revision_id": "1",
            "data": {
                "package": {
                    "name": "Name",
                    "id": "Name",
                    "url": "https://url.com"
                }
            },
            "test3": "3",
            "test4": "4"
        }
    ]
    def package_show(id): return package_info

    await monitor.watch_changed_packages(recently_changed, list_checked_packages(), package_show)


# async func for testing
# if it's called in the live code with an await
@pytest.mark.asyncio
async def test_get_resources(printer, router, package_info, monitor):
    content = FakeObject()
    content.text = "Fake text??"
    def get_data(url): return content
    ini = DoorstepIni(context_package=package_info)
    await monitor.get_resource(ini, get_data)


def test_check_empty_resources():
    pass
