import click
import gettext

from ltldoorstep import printer
from ltldoorstep.wamp_client import launch_wamp

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
@click.option('-m', '--metadata', default=None)
@click.argument('filename', 'data file to process')
@click.argument('workflow', 'Python workflow module')
@click.pass_context
def process(ctx, filename, workflow, metadata):
    printer = ctx.obj['printer']
    bucket = ctx.obj['bucket']

    launch_wamp(filename, workflow, printer, metadata)

    print(printer.print_output())
