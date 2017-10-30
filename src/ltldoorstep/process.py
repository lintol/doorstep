from dask.distributed import Client
import os
import pandas as pd
import tabulate
import logging
import colorama
from . import csv

class CsvProcessor:
    def print_report(self, printer, result):
        levels = {
            logging.INFO: [],
            logging.WARNING: [],
            logging.ERROR: []
        }

        for comment, detail in result.items():
            levels[detail[0]].append([
                comment,
                str(detail[1]) if detail[1] else ''
            ])

        output_sections = []
        if levels[logging.ERROR]:
            printer.add_section('\n'.join([
                'Errors',
                tabulate.tabulate(levels[logging.ERROR]),
            ]), colorama.Fore.RED + colorama.Style.BRIGHT)

        if levels[logging.WARNING]:
            printer.add_section('\n'.join([
                'Warnings',
                tabulate.tabulate(levels[logging.WARNING]),
            ]), colorama.Fore.YELLOW + colorama.Style.BRIGHT)

        if levels[logging.INFO]:
            printer.add_section('\n'.join([
                'Information',
                tabulate.tabulate(levels[logging.INFO])
            ]))

    def run(self, filename):
        df = pd.read_csv(filename)

        client = Client('tcp://localhost:8786')
        result = {}
        for step in csv.get_workflow():
            result.update(client.submit(step, df).result())

        return result
