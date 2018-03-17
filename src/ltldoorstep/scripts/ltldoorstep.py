import click
import json
import requests
import gettext
from ltldoorstep import printer
from ltldoorstep.engines import engines
from ltldoorstep.file import make_file_manager
import asyncio
import logging

def get_engine(engine):
    if ':' in engine:
        engine, engine_options = engine.split(':')
        sp = lambda x: (x.split('=') if '=' in x else (x, True))
        engine_options = {k: v for k, v in map(sp, engine_options.split(','))}
    else:
        engine_options = {}

    return engine, engine_options

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('-b', '--bucket', default=None)
@click.option('-o', '--output', type=click.Choice(['ansi', 'json']), default='ansi')
@click.option('--output-file', default=None)
@click.pass_context
def cli(ctx, debug, bucket, output, output_file):
    if output == 'json':
        prnt = printer.JsonPrinter(debug, target=output_file)
    elif output == 'ansi':
        prnt = printer.TermColorPrinter(debug, target=output_file)
    else:
        raise RuntimeError(_("Unknown output format"))

    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
    logger = logging.getLogger(__name__)

    ctx.obj = {
        'DEBUG': debug,
        'printer': prnt,
        'bucket': bucket,
        'logger': logger
    }
    gettext.install('ltldoorstep')

@cli.command(name='engine-info')
@click.argument('engine', 'engine to get information about')
@click.pass_context
def engine_info(ctx, engine=None):
    if engine:
        if engine in engines:
            click.echo(_('Engine details:') + ' ' + engine + "\n")
            config_help = engines[engine].config_help()
            if config_help:
                for setting, description in config_help.items():
                    click.echo("%s:\n\t%s" % (setting, description))
            else:
                click.echo("No configuration settings for this engine")
        else:
            click.echo(_('Engine not known'))
    else:
        click.echo(_('Engines available:') + ' ' + ', '.join(engines))

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
@click.argument('filename', 'data file to process')
@click.argument('workflow', 'Python workflow module')
@click.option('-e', '--engine', required=True)
@click.option('-m', '--metadata', default=None)
@click.pass_context
def process(ctx, filename, workflow, engine, metadata):
    printer = ctx.obj['printer']
    bucket = ctx.obj['bucket']

    engine, engine_options = get_engine(engine)
    click.echo(_("Engine: %s" % engine))
    engine = engines[engine](config=engine_options)

    if metadata is None:
        metadata = {}
    else:
        with open(metadata, 'r') as metadata_file:
            metadata = json.load(metadata_file)

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(engine.run(filename, workflow, metadata, bucket=bucket))
    printer.build_report(result)

    printer.print_output()

@cli.command()
@click.option('--engine', required=True)
@click.option('--protocol', type=click.Choice(['http', 'wamp']), required=True)
@click.option('--router', default='localhost:8080')
@click.pass_context
def serve(ctx, engine, protocol, router):
    printer = ctx.obj['printer']

    engine, engine_options = get_engine(engine)
    click.echo(_("Engine: %s" % engine))
    engine = engines[engine](config=engine_options)

    if protocol == 'http':
        from ltldoorstep.flask_server import launch_flask
        launch_flask(engine)
    elif protocol == 'wamp':
        from ltldoorstep.wamp_server import launch_wamp
        launch_wamp(engine, router)
    else:
        raise RuntimeError(_("Unknown protocol"))

@cli.command()
@click.argument('workflow', 'Python workflow module')
@click.option('--url', required=True)
@click.option('--engine', required=True)
@click.pass_context
def crawl(ctx, workflow, url, engine):
    printer = ctx.obj['printer']

    engine, engine_options = get_engine(engine)
    click.echo(_("Engine: %s" % engine))
    engine = engines[engine](config=engine_options)

    metadata = {}

    from ckanapi import RemoteCKAN
    client = RemoteCKAN(url, user_agent='lintol-doorstep-crawl/1.0 (+http://lintol.io)')
    resources = client.action.resource_search(query='format:csv')
    if 'results' in resources:
        for resource in resources['results']:
            r = requests.get(resource['url'])
            with make_file_manager(content={'data.csv': r.text}) as file_manager:
                filename = file_manager.get('data.csv')
                loop = asyncio.get_event_loop()
                result = loop.run_until_complete(engine.run(filename, workflow, metadata))
                printer.build_report(result)
    printer.print_output()
