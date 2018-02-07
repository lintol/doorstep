import colorama
import logging
import tabulate
import json

class Printer:
    def __init__(self, debug=False, target=None):
        self._output_sections = []
        self._debug = debug
        self._target = target

    def get_debug(self):
        return self._debug

    def build_report():
        raise NotImplementedError("No report builder implemented for this printer")

    def get_output():
        raise NotImplementedError("No outputter implemented for this printer")

    def print_output(self):
        output = self.get_output()

        if self._target is None:
            print(output)
        else:
            with open(self._target, 'w') as target_file:
                target_file.write(output)

class TermColorPrinter(Printer):
    def get_output(self):
        return '\n\n'.join(self._output_sections)

    def build_report(self, result_sets):
        levels = {
            logging.INFO: [],
            logging.WARNING: [],
            logging.ERROR: []
        }

        general_output = []
        results = []
        for result_set in result_sets['tables'][0]['warnings']:
            try:
                results.append([result_set['item'], logging.ERROR, result_set['code'], result_set['message']])
            except ValueError:
                self.add_section(result_set)

        for detail in results:
            levels[detail[1]].append([
                detail[0],
                str(detail[2]) if detail[2] else '',
                detail[3]
            ])

        output_sections = []
        if levels[logging.ERROR]:
            self.add_section('\n'.join([
                'Errors',
                tabulate.tabulate(levels[logging.ERROR]),
            ]), colorama.Fore.RED + colorama.Style.BRIGHT)

        if levels[logging.WARNING]:
            self.add_section('\n'.join([
                'Warnings',
                tabulate.tabulate(levels[logging.WARNING]),
            ]), colorama.Fore.YELLOW + colorama.Style.BRIGHT)

        if levels[logging.INFO]:
            self.add_section('\n'.join([
                'Information',
                tabulate.tabulate(levels[logging.INFO])
            ]))

    def add_section(self, output, style=None):
        if style:
            output = style + output + colorama.Style.RESET_ALL
        self._output_sections.append(output)


class JsonPrinter(Printer):
    def get_output(self):
        return self._output

    def build_report(self, result_sets):
        self._output = json.dumps(result_sets)
