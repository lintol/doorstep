import time
import requests
import asyncio
import json
import ltldoorstep.printer as printer
import time
from ltldoorstep.file import make_file_manager
from ltldoorstep.ini import DoorstepIni
from ltldoorstep.wamp_client import launch_wamp
from ltldoorstep.crawler import announce_resource

# time delay could be user defined
TIME_DELAY = 5

async def search_gather(client, watch_changed_packages, settings):
    cursor = 0
    complete = None
    while not complete:
        settings['start'] = cursor
        try:
            packages = client.package_search(**settings)
        except client.exception as exp:
            print(exp)
            # catches connection errors only
            print('Error retrieving from client API [revision-list], trying again...')
            time.sleep(1)
        else:
            cursor += len(packages['results'])
            complete = cursor > packages['count']

        list_checked_packages = []

        recent_revisions = [{'revision_id': package['id'], 'data': {'package': package}} for package in packages['results']]
        print(f"Total packages: {len(recent_revisions)}")

        # calls another async fucntion using the vars set above
        await watch_changed_packages(
            recent_revisions, # list of dicts returned from client
            list_checked_packages, # list that is added to as program runs
            client.package_show # data sent to get_resources
        )

async def crawl_gather(client, watch_changed_packages):
    try:
        packages = client.package_list()
    except client.exception as exp:
        print(exp)
        # catches connection errors only
        print('Error retrieving from client API [revision-list], trying again...')
        time.sleep(1)

    list_checked_packages = []

    recent_revisions = [{'revision_id': package, 'data': {'package': {'id': package}}} for package in packages['results']]
    print(f"Total packages: {len(recent_revisions)}")

    # calls another async fucntion using the vars set above
    await watch_changed_packages(
        recent_revisions, # list of dicts returned from client
        list_checked_packages, # list that is added to as program runs
        client.package_show # data sent to get_resources
    )

async def watch_gather(client, watch_changed_packages):
    # runs code from old commit that uses the client to get the list of changed packages
    list_checked_packages = []

    while True:
        try:
            recently_changed = client.recently_changed_packages_activity_list()
        except client.exception:
            # catches connection errors only
            print('Error retrieving from client API [recently-changed-packages-activity-list], trying again...')
            time.sleep(1)

        print("Waiting - ", TIME_DELAY)
        time.sleep(TIME_DELAY)

        desirable = []
        for recent in recently_changed:
            if recent['activity_type'] == 'deleted package':
                continue

            desirable.append(recent)

        # calls another async fucntion using the vars set above
        await watch_changed_packages(
            desirable, # list of dicts returned from client
            list_checked_packages, # list that is added to as program runs
            client.package_show # data sent to get_resources
        )

class Monitor:
    """ Monitor class acts as the interface for WAMP
    handles functionality that checks for new packages & retrives resources from the client
    """
    def __init__(self, cmpt, client, printer, gather_fn):
        self.cmpt = cmpt # create the component
        self.client = client # creates the client from data_store. could be either dummy or ckan obj
        self.printer = printer
        self.gather_fn = gather_fn

    async def run(self):
        await self.gather_fn(self.client, self.watch_changed_packages)

    async def watch_changed_packages(self, recently_changed, list_checked_packages, package_show):
        """
        Will run as long as the watch option is used in ltlwampclient.py
        Note 'dataset' & 'package' are interchangable terms
        """
        # iterates through recently changed packages obtained from ckanapi
        for changed in recently_changed:
            changed_package_revision_id = changed['revision_id']
            # checks if the id is in the list
            if changed_package_revision_id not in list_checked_packages:
                # var set to false so the while loop runs until package_show() works
                # to prevent any issues with retrieving a dataset & the code overlooking it during the next cycle
                retrieved = False
                while not retrieved:
                    print(changed['data']['package']['id'])
                    try:
                        package_info = package_show(id=changed['data']['package']['id'])
                        retrieved = True
                    except self.client.exception as exp:
                        print(exp)
                        # catches connection errors only
                        print('Error retrieving from client API [package-show], trying again...')
                        time.sleep(1)

                ini = DoorstepIni(context_package=package_info) # classes = studley case
                # calls async function from Monitor class to get the dataset's resource using the package info
                await self.get_resource(ini, requests.get)

                # when the code runs succesfully and the resource is retreived,
                # it adds it to the list so it's not duplicated
                list_checked_packages.append(changed_package_revision_id)  # list of names
        print('----')

    async def get_resource(self, ini, rg_func):
        """
        Get the URL from the dataset resources & create a local file with the results
        """
        # uses the Monitor class, ini obj & request.get function

        # get the package_show data based on the name of the changed dataset
        print("Getting resource from package")
        for resource in ini.package['resources']:
            # loops through resources in the package
            source = self.client.get_identifier()
            # finds where the resource is coming from, ie ckan or dummy
            print(f'Announcing resource: {resource["url"]} from {source}')
            # calls async function that doesn't create a report, but gets the data???
            await announce_resource(self.cmpt, resource, ini, source)


async def monitor_for_changes(cmpt, client, printer, gather_fn):
    """
    creates Monitor object
    """
    monitor = Monitor(cmpt, client, printer, gather_fn)
    await monitor.run()
