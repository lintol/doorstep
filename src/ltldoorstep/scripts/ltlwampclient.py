import click
import asyncio
import pandas
import time
import datetime
import json
import csv
import logging
import gettext
import requests
from ltldoorstep import printer
from ltldoorstep.file import make_file_manager
from ltldoorstep.wamp_client import launch_wamp, announce_wamp

from ltldoorstep.crawler import execute_workflow, do_crawl

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

    runner = launch_wamp(execute_workflow, router_url, filename, workflow, printer, metadata)

    print(printer.print_output())

@cli.command()
@click.argument('workflow', 'Python workflow module', default=None, required=False)
@click.option('--url', required=True)
@click.option('--watch/--no-watch', help='Should this keep running indefinitely?', default=False)
@click.option('--watch-refresh-delay', help='How long until this calls the given CKAN target again', default='60s')
@click.option('--publish/--no-publish', default=False)
@click.pass_context
def crawl(ctx, workflow, url, watch, watch_refresh_delay, publish):
    """
    Crawl function gets the URL of all packages in the CKAN instance.
    """
    printer = ctx.obj['printer']
    router_url = ctx.obj['router_url']
    logging.warn("printer  & url types")
    logging.warn(type(printer))
    logging.warn(type(router_url))

    ini = None

    if watch:
        from ckanapi import RemoteCKAN
        client = RemoteCKAN(url, user_agent='lintol-doorstep-crawl/1.0 (+http://lintol.io)')

        watch(ctx, workflow, url, client)
    else:
        loop = asyncio.get_event_loop()

        async def _exec(cmpt):
            try:
                await do_crawl(cmpt, url, workflow, printer, publish)
            except Exception as e:
                loop = asyncio.get_event_loop()
                loop.stop()
                raise e

        loop.run_until_complete(launch_wamp(_exec, router_url))
        loop.run_forever()

@cli.command()
@click.argument('workflow', 'Python workflow module')
@click.option('--url', required=True)
@click.pass_context
def watch(ctx, workflow, url, client):
    while True:
        starting_package_list = client.action.package_list()
        for package in starting_package_list:
            logging.warn("Getting %s & waiting 5 seconds" % package)
            time.sleep(5)
            package_metadata = client.action.package_show(id=package)
            # ^ json objs
            ini = DoorstepIni(context_package=package_metadata) # classes = studley case
            resources = ini.package['resources']
            # ^ creating a list
            current_timestamp = datetime.datetime.now()
            logging.warn("finding type of current timestamp")
            logging.warn(type(current_timestamp))
            for resource in resources: # loops through list of resources to get response obj
                # resource is a dict
                logging.warn("Getting resource from package - %s " % resource )
                r = requests.get(resource['url'])
                time = resource['created']
                converted_timestamp = pandas.to_datetime(time)
                logging.warn("finding type of resource timestamp")
                logging.warn(type(time))
                # need to sort out types to find the difference between times
                # time_difference = time_string - resource['created']
                # logging.warn(time_difference)
                difference = datetime.timedelta(current_timestamp - converted_timestamp)
                logging.warn("DIFFERENCE")
                logging.warn(difference)
                with make_file_manager(content={'data.csv': r.text}) as file_manager:
                    try:
                        filename = file_manager.get('data.csv')
                        result = launch_wamp(execute_workflow, router_url, filename, workflow, printer, ini)
                        # launch_wamp connects to crossbar (which acts as the wamp router) to communicate with the ckan instance
                        if result:
                            printed_result = printer.build_report(result)
                            print(printed_result)
                    except Exception as e:
                        logging.error("launch_wamp error - %s" % e)
        logging.warn("waiting 5 secs")
        time.sleep(5)
        printer.print_output()


@cli.command()
@click.argument('workflow', 'Python workflow module')
@click.argument('package', 'Package ID')
@click.option('--url', required=True)
@click.option('--watch/--no-watch', help='Should this keep running indefinitely?', default=False)
@click.option('--watch-refresh-delay', help='How long until this calls the given CKAN target again', default='60s')
@click.option('--watch-persist-to', default=None)
@click.pass_context
def find_package(ctx, workflow, package, url, watch, watch_refresh_delay, watch_persist_to):
    """
    Find Package searches for a specific package and returns the information as metadata
    """
    printer = ctx.obj['printer']
    router_url = ctx.obj['router_url']

    if watch_persist_to:
        watch = True
    ini = None
    
    from ckanapi import RemoteCKAN
    from ckanapi.errors import CKANAPIError
    client = RemoteCKAN(url, user_agent='lintol-doorstep-crawl/1.0 (+http://lintol.io)')
    if not watch:
        resources = client.action.resource_search(query='name:' + package)
        print(resources)
        if 'results' in resources:
            for resource in resources['results']:
                r = requests.get(resource['url'])
                with make_file_manager(content={'data.csv': r.text}) as file_manager:
                    filename = file_manager.get('data.csv')
                    result = launch_wamp(execute_workflow, router_url, filename, workflow, printer, ini)
                    print(result)
                    if result:
                        printer.build_report(result)
        printer.print_output()
