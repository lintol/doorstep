import requests
import logging
import asyncio
import json
import ltldoorstep.printer as printer
import time
from ltldoorstep.file import make_file_manager
from ltldoorstep.ini import DoorstepIni
from ltldoorstep.crawler import announce_resource

# time delay could be user defined
TIME_DELAY = 5
RETRIES = 10

async def search_gather(client, watch_changed_packages, settings, time_delay, skip, cmpt_wrap):
    cursor = skip
    complete = None
    while not complete:
        logging.info("Gathering from %d", cursor)

        async def iteration(cmpt, cursor, complete):
            settings['start'] = cursor

            retry = 1
            while retry:
                try:
                    packages = client.package_search(**settings)
                    # package search start = 25
                    # gives an argument to that search
                    # args don't hvae to be hard coded
                    # loops through each page and starts the search again with 'cursor' moved
                    # brings in a limited number of results & cursor increases based on the results returned
                    retry = False
                except client.exception as exp:
                    logging.error(exp)
                    if retry > RETRIES:
                        raise exp

                    # catches connection errors only
                    logging.error('Error retrieving from client API [revision-list], trying again...')
                    time.sleep(1)
                    retry += 1
                else:
                    cursor += len(packages['results'])
                    complete = cursor >= packages['count']

            list_checked_packages = []

            recent_revisions = []
            results = packages['results']
            for package in results:
                recent_revisions.append({'revision_id': package['id'], 'data': {'package': package}})

            logging.info(f"Total packages: {len(recent_revisions)}")

            # calls another async fucntion using the vars set above
            await watch_changed_packages(
                recent_revisions, # list of dicts returned from client
                list_checked_packages, # list that is added to as program runs
                client.package_show, # data sent to get_resources
                cmpt
            )

            return cursor, complete

        cursor, complete = await cmpt_wrap(iteration, cursor, complete)

        if not complete:
            logging.info("Waiting - %d", time_delay)
            time.sleep(time_delay)

async def crawl_gather(client, watch_changed_packages, time_delay, skip, cmpt_wrap):
    try:
        packages = client.package_list()
    except client.exception as exp:
        logging.info(exp)
        # catches connection errors only
        logging.info('Error retrieving from client API [revision-list], trying again...')
        time.sleep(1)

    list_checked_packages = []

    recent_revisions = [{'revision_id': package, 'data': {'package': {'id': package}}} for package in packages['results']]
    logging.info(f"Total packages: {len(recent_revisions)}")

    async def iteration(cmpt):
        # calls another async fucntion using the vars set above
        await watch_changed_packages(
            recent_revisions, # list of dicts returned from client
            list_checked_packages, # list that is added to as program runs
            client.package_show, # data sent to get_resources
            cmpt
        )
    await cmpt_wrap(iteration)

async def watch_gather(client, watch_changed_packages, time_delay, skip, cmpt_wrap):
    # runs code from old commit that uses the client to get the list of changed packages
    list_checked_packages = []

    while True:
        async def iteration(cmpt):
            recently_changed = []
            try:
                recently_changed = client.recently_changed_packages_activity_list()
            except client.exception:
                # catches connection errors only
                logging.warning('Error retrieving from client API [recently-changed-packages-activity-list], trying again...')
                time.sleep(1)

            desirable = []
            for recent in recently_changed:
                if recent['activity_type'] == 'deleted package':
                    continue

                desirable.append(recent)

            # calls another async fucntion using the vars set above
            await watch_changed_packages(
                desirable, # list of dicts returned from client
                list_checked_packages, # list that is added to as program runs
                client.package_show, # data sent to get_resources
                cmpt
            )

        await cmpt_wrap(iteration)
        logging.info("Waiting - %d", time_delay)
        time.sleep(time_delay)

class Monitor:
    """ Monitor class acts as the interface for WAMP
    handles functionality that checks for new packages & retrives resources from the client
    """
    def __init__(self, cmpt, client, printer, gather_fn, announce_fn, update=False, time_delay=None, skip=0):
        self.cmpt = cmpt # create the component
        self.client = client # creates the client from data_store. could be either dummy or ckan obj
        self.printer = printer
        self.gather_fn = gather_fn
        self.announce_fn = announce_fn
        self.update = update
        self.skip = skip
        if time_delay is None:
            self.time_delay = TIME_DELAY
        else:
            self.time_delay = time_delay

    async def run(self):
        await self.gather_fn(self.client, self.watch_changed_packages, self.time_delay, self.skip, self.cmpt)
        logging.info("Completed gathering")

    async def watch_changed_packages(self, recently_changed, list_checked_packages, package_show, cmpt):
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

                retry = 1
                while not retrieved and retry < RETRIES:
                    try:
                        package_info = package_show(id=changed['data']['package']['id'])
                        retrieved = True
                    except self.client.exception as exp:
                        logging.error(exp)
                        # catches connection errors only
                        logging.error('Error retrieving from client API [package-show], trying again...')
                        time.sleep(0.7)
                        retry += 1

                    time.sleep(0.3)
                if not retrieved:
                    logging.error("Package - %s: NOT RETRIEVED (%s)", package_info['name'], changed['data']['package']['id'])
                    continue

                logging.info("Package - %s", package_info['name'])

                ini = DoorstepIni(context_package=package_info) # classes = studley case
                # calls async function from Monitor class to get the dataset's resource using the package info
                await self.get_resource(ini, requests.get, cmpt)

                # when the code runs succesfully and the resource is retreived,
                # it adds it to the list so it's not duplicated
                list_checked_packages.append(changed_package_revision_id)  # list of names

    async def get_resource(self, ini, rg_func, cmpt):
        """
        Get the URL from the dataset resources & create a local file with the results
        """
        # uses the Monitor class, ini obj & request.get function

        # get the package_show data based on the name of the changed dataset
        logging.info("Getting resource from package")
        for resource in ini.package['resources']:
            # loops through resources in the package
            source = self.client.get_identifier()
            # finds where the resource is coming from, ie ckan or dummy
            logging.info(f'Announcing resource: {resource["url"]} from {source}')
            # calls async function that doesn't create a report, but gets the data???
            await self.announce_fn(cmpt, resource, ini, source, self.update)


async def monitor_for_changes(cmpt, client, printer, gather_fn, update=False, time_delay=None, skip=0):
    """
    creates Monitor object
    """
    monitor = Monitor(cmpt, client, printer, gather_fn, announce_resource, update=update, time_delay=time_delay, skip=skip)
    await monitor.run()
