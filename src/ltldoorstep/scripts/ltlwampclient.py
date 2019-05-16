import click
import json
import logging
import gettext
import requests

from ltldoorstep import printer
from ltldoorstep.file import make_file_manager
from ltldoorstep.wamp_client import launch_wamp
from ltldoorstep.ini import DoorstepIni

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

    launch_wamp(router_url, filename, workflow, printer, metadata)

    print(printer.print_output())

@cli.command()
@click.argument('workflow', 'Python workflow module')
@click.option('--url', required=True)
@click.option('--watch/--no-watch', help='Should this keep running indefinitely?', default=False)
@click.option('--watch-refresh-delay', help='How long until this calls the given CKAN target again', default='60s')
@click.option('--watch-persist-to', default=None)
@click.pass_context
def crawl(ctx, workflow, url, watch, watch_refresh_delay, watch_persist_to):
    printer = ctx.obj['printer']
    router_url = ctx.obj['router_url']

    if watch_persist_to:
        watch = True

    ini = None

    from ckanapi import RemoteCKAN
    client = RemoteCKAN(url, user_agent='lintol-doorstep-crawl/1.0 (+http://lintol.io)')

    if not watch:
        resources = client.action.resource_search(query='format:csv')
        print(resources)
        if 'results' in resources:
            for resource in resources['results']:
                r = requests.get(resource['url'])
                with make_file_manager(content={'data.csv': r.text}) as file_manager:
                    filename = file_manager.get('data.csv')
                    result = launch_wamp(router_url, filename, workflow, printer, ini)
                    print(result)
                    if result:
                        printer.build_report(result)
        printer.print_output()
    else:
        # logging.warn('**** in the else block')
        packages = client.action.package_list()
        logging.warn('**** in the else block')
        for package in packages:
            logging.warn("Package name? %s" % package)
            package_metadata = client.action.package_show(id=package)
            ini = DoorstepIni(context_package=package_metadata)
            resources = ini.package['resources']
            for resource in resources:
                r = requests.get(resource['url'])
                with make_file_manager(content={'data.csv': r.text}) as file_manager:
                    filename = file_manager.get('data.csv')
                    result = launch_wamp(router_url, filename, workflow, printer, ini)
                    print(result)
                    if result:
                        printer.build_report(result)
                    else:
                        printer.build_report("Nope")
        printer.print_output()
