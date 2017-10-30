import click
from ltldoorstep import printer
from ltldoorstep import process

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
@click.argument('filename', 'CSV file to process')
@click.pass_context
def csv(ctx, filename=None):
    printer = ctx.obj['printer']

    click.echo('PROCESS CSV')

    processor = process.CsvProcessor()
    result = processor.run(filename)
    processor.print_report(printer, result)

    print(printer.get_output())
