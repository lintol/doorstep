"""
Module to access the wamp router (Crossbar) to query the CKAN server
"""
import logging
import asyncio
import gettext
import click
import time
import datetime
import json
import csv
import gettext
import requests
from ltldoorstep import printer
from ltldoorstep.ini import DoorstepIni
from ltldoorstep.file import make_file_manager
from ltldoorstep.wamp_client import launch_wamp, announce_wamp
from ltldoorstep.data_store import CkanDataStore, DummyDataStore
from ltldoorstep.crawler import execute_workflow, do_crawl
from ltldoorstep.wamp_client import launch_wamp
from ltldoorstep.watch import monitor_for_changes, watch_gather, crawl_gather, search_gather

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('-b', '--bucket', default=None)
@click.option('--router-url', default='#ws://localhost:8080/ws')
@click.pass_context
def cli(ctx, debug, bucket, router_url):
    prnt = printer.TermColorPrinter(debug)
    ctx.obj = {
        'DEBUG': debug,
        'printer': prnt,
        'bucket': bucket,
        'router_url': router_url
    }
    gettext.install('ltldoorstep')


@cli.command()
@click.pass_context
def status(ctx):
    debug = ctx.obj['DEBUG']
    click.echo(_('STATUS'))

    if debug:
        click.echo(_('Debug is on'))
    else:
        click.echo(_('Debug is off'))


@cli.command()
@click.option('-m', '--metadata', default=None)
@click.argument('filename', 'data file to process')
@click.argument('workflow', 'Python workflow module')
@click.pass_context
def process(ctx, filename, workflow, metadata):
    printer = ctx.obj['printer']
    bucket = ctx.obj['bucket']
    router_url = ctx.obj['router_url']

    async def _exec(cmpt):
        try:
            await execute_workflow(cmpt, filename, workflow, metadata)
        except Exception as e:
            # if there is any exception thrown, it stops everything
            loop = asyncio.get_event_loop() 
            # stops any async events running
            loop.stop()
            # throws exception to the top of the stack
            raise e

    loop = asyncio.get_event_loop() # finds whatever async stuff is happening
    loop.run_until_complete(launch_wamp(_exec, router_url)) # runs the wamp server
    loop.run_forever() # forever

    print(printer.print_output())


@cli.command()
@click.argument('workflow', 'Python workflow module', default=None, required=False)
@click.option('--url', required=True)
@click.option('--search', required=False, default=None)
@click.option('--watch/--no-watch', help='Should this keep running indefinitely?', default=False)
@click.option('--watch-refresh-delay', help='How long until this calls the given CKAN target again', default='60s')
@click.option('--publish/--no-publish', default=False)
@click.option('--dummy-ckan/--no-dummy-ckan', default=False)
@click.option('--force-update/--no-force-update', default=False)
@click.pass_context
def crawl(ctx, workflow, url, search, watch, watch_refresh_delay, publish, dummy_ckan, force_update):
    """
    Crawl function gets the URL of all packages in the CKAN instance.
    Adding the 'watch' option only gets it to look for datasets added/altered since crawl started to run.
    Add a timedelay argument to give a custom time delay
    """
    printer = ctx.obj['printer']
    router_url = ctx.obj['router_url']
    logging.warn("printer  & url types")
    logging.warn(type(printer))
    logging.warn(type(router_url))

    loop = asyncio.get_event_loop()

    if watch:
        gather_fn = watch_gather
    else:
        gather_fn = crawl_gather

    if search:
        if watch:
            raise RuntimeError("Can only use watch or search")
        search_settings = json.loads(search)
        gather_fn = lambda c, w: search_gather(c, w, search_settings)

    if dummy_ckan:
        client = DummyDataStore()
    else:
        client = CkanDataStore(url)

    # launch_wamp connects to crossbar (which acts as the wamp router) to communicate with the ckan instance
    # runs the component, which is what runs the code from watch.py's Monitor class??
    async def _exec(cmpt):
        try:
            # calls async function to create monitor object
            await monitor_for_changes(
                cmpt,
                client,
                printer,
                gather_fn,
                force_update
            )
        except Exception as e:
            # stops the code regardless of the exception thrown
            loop = asyncio.get_event_loop()
            loop.stop()
            raise e # throws to top of stack
        await cmpt.leave(u'wamp.close.complete', 'finished')
        loop = asyncio.get_event_loop()
        loop.stop()

    # runs the wamp server forever more or less
    loop.run_until_complete(launch_wamp(_exec, router_url))
    loop.run_forever()

@cli.command()
@click.argument('workflow', 'Python workflow module')
@click.argument('package', 'Package ID')
@click.option('--url', required=True)
@click.option('--dummy-ckan/--no-dummy-ckan', default=False)
@click.pass_context
def find_package(ctx, workflow, package, url, watch, watch_refresh_delay, watch_persist_to, dummy_ckan):
    """
    Find Package searches for a specific package and returns the information as metadata
    Arguments:
        WORKFLOW    Python workflow module (.py) to run against the data.
    """
    printer = ctx.obj['printer']
    router_url = ctx.obj['router_url']

    ini = DoorstepIni()

    if dummy_ckan:
        client = DummyDataStore()
        # creates dummy object
    else:
        client = CkanDataStore(url)
        # creates ckan object

    loop = asyncio.get_event_loop()
    # runs gather resources continuously
    loop.run_until_complete(gather_resources(client, package, workflow, router_url, ini, printer))
    loop.run_forever()

async def gather_resources(client, package, workflow, router_url, metadata, printer):
    # calls code from Monitor class that runs everything
    async def _exec(cmpt, filename):
        try:
            # calls async function to follow steps to create report
            await execute_workflow(cmpt, filename, workflow, metadata)
        except Exception as e:
            loop = asyncio.get_event_loop()
            loop.stop()
            raise e # throws exception to top of stack & stops code if there's any error

    resources = client.resource_search(query='name:' + package)
    # searches based on name input
    print(resources)
    if 'results' in resources:
        # if resources have column called 'results'
        for resource in resources['results']:
            # loops through resources' results
            r = requests.get(resource['url'])
            # makes a response object using the url column
            with make_file_manager(content={'data.csv': r.text}) as file_manager:
                # makes a file
                filename = file_manager.get('data.csv')
                result = launch_wamp(lambda cmpt: _exec(cmpt, filename), router_url)
                # launches the wamp server, creates a component using _exec()
                print(result)
                if result:
                    printer.build_report(result)
    printer.print_output()

