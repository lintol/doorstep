import colorama

class TermColorPrinter:
    def __init__(self, debug=False):
        self._output_sections = []
        self._debug = debug

    def get_debug(self):
        return self._debug

    def get_output(self):
        return '\n\n'.join(self._output_sections)

    def add_section(self, output, style=None):
        if style:
            output = style + output + colorama.Style.RESET_ALL
        self._output_sections.append(output)
