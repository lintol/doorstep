import click
from ltldoorstep import printer
from ltldoorstep.engines import engines

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def cli(ctx, debug):
    prnt = printer.TermColorPrinter(debug)
    ctx.obj = {
        'DEBUG': debug,
        'printer': prnt
    }

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

    print(engine)
    engine = engines[engine]()
    result = engine.run(filename, workflow)
    printer.print_report(result)

    print(printer.get_output())
