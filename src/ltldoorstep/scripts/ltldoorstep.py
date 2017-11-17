import click
import gettext
from ltldoorstep import printer
from ltldoorstep.engines import engines
import asyncio

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('-b', '--bucket', default=None)
@click.pass_context
def cli(ctx, debug, bucket):
    prnt = printer.TermColorPrinter(debug)
    ctx.obj = {
        'DEBUG': debug,
        'printer': prnt,
        'bucket': bucket
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
@click.argument('filename', 'data file to process')
@click.argument('workflow', 'Python workflow module')
@click.option('--engine', type=click.Choice(engines.keys()), required=True)
@click.pass_context
def process(ctx, filename, workflow, engine):
    printer = ctx.obj['printer']
    bucket = ctx.obj['bucket']

    click.echo(_("Engine: %s" % engine))
    engine = engines[engine]()

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(engine.run(filename, workflow, bucket=bucket))
    printer.print_report(result)

    print(printer.get_output())

@cli.command()
@click.option('--engine', type=click.Choice(engines.keys()), required=True)
@click.option('--protocol', type=click.Choice(['http', 'wamp']), required=True)
@click.pass_context
def serve(ctx, engine, protocol):
    printer = ctx.obj['printer']

    click.echo(_("Engine: %s" % engine))

    engine = engines[engine]()

    if protocol == 'http':
        from ltldoorstep.flask_server import launch_flask
        launch_flask(engine)
    elif protocol == 'wamp':
        from ltldoorstep.wamp_server import launch_wamp
        launch_wamp(engine)
    else:
        raise RuntimeError(_("Unknown protocol"))

