import colorama
import logging
import tabulate

class TermColorPrinter:
    def __init__(self, debug=False):
        self._output_sections = []
        self._debug = debug

    def get_debug(self):
        return self._debug

    def get_output(self):
        return '\n\n'.join(self._output_sections)

    def print_report(self, result_sets):
        levels = {
            logging.INFO: [],
            logging.WARNING: [],
            logging.ERROR: []
        }

        results = {}
        for result_set in result_sets:
            results.update(result_set)

        for comment, detail in results.items():
            levels[detail[1]].append([
                detail[0],
                str(detail[2]) if detail[2] else '',
                comment
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
