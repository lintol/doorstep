import os
import json
from autobahn.asyncio.wamp import ApplicationRunner, ApplicationSession
import asyncio

class WampClientComponent(ApplicationSession):
    """Connector to join and execute a WAMP session."""

    _server = None
    _session = None

    def __init__(self, filename, workflow, *args, printer=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = filename
        self._workflow = workflow
        self._printer = printer

    async def onJoin(self, details):
        """When we join the server, execute the client workflow."""

        with open(self._filename, 'r') as file_obj:
            content = file_obj.read()
        filename = os.path.basename(self._filename)

        with open(self._workflow, 'r') as file_obj:
            module = file_obj.read()
        workflow = os.path.basename(self._workflow)

        self._server, self._session = await self.call('com.ltldoorstep.engage')

        await self.call_server('processor.post', filename, content)
        await self.call_server('data.post', workflow, module)

        result = json.loads(await self.call_server('report.get'))

        if self._printer:
            self._printer.print_report(result)

        # Stop and return control
        loop = asyncio.get_event_loop()
        loop.stop()

    async def call_server(self, endpoint, *args, **kwargs):
        """Generate the correct endpoint for the known server."""

        return await self.call(
            'com.ltldoorstep.{server}.{endpoint}'.format(server=self._server, endpoint=endpoint),
            self._session,
            *args,
            **kwargs
        )


def launch_wamp(filename, workflow, printer):
    """Run the workflow against a WAMP server."""

    runner = ApplicationRunner(url='ws://localhost:8080/ws', realm='realm1')
    runner.run(lambda *args, **kwargs: WampClientComponent(
        filename,
        workflow,
        *args,
        printer=printer,
        **kwargs
    ))
