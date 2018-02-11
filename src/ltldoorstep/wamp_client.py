import os
import uuid
import json
import json
from autobahn.asyncio.wamp import ApplicationRunner, ApplicationSession
import asyncio

class WampClientComponent(ApplicationSession):
    """Connector to join and execute a WAMP session."""

    _server = None
    _session = None

    def __init__(self, filename, workflow, metadata, *args, printer=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = filename
        self._workflow = workflow
        self._metadata = metadata
        self._printer = printer

    async def onJoin(self, details):
        """When we join the server, execute the client workflow."""

        with open(self._filename, 'r') as file_obj:
            content = file_obj.read()
        filename = os.path.basename(self._filename)

        with open(self._workflow, 'r') as file_obj:
            module = file_obj.read()
        workflow = os.path.basename(self._workflow)

        if self._metadata is None:
            metadata = {
                'definitions': {
                    str(uuid.uuid4()): {
                        'module': workflow
                    }
                }
            }
        else:
            with open(self._metadata, 'r') as file_obj:
                metadata = json.load(file_obj)

        self._server, self._session = await self.call('com.ltldoorstep.engage')

        await self.call_server('processor.post', {workflow: module}, metadata)
        await self.call_server('data.post', filename, content)

        result = json.loads(await self.call_server('report.get'))

        if self._printer:
            self._printer.build_report(result)

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


def launch_wamp(filename, workflow, printer, metadata):
    """Run the workflow against a WAMP server."""

    runner = ApplicationRunner(url='ws://localhost:8080/ws', realm='realm1')
    runner.run(lambda *args, **kwargs: WampClientComponent(
        filename,
        workflow,
        metadata,
        *args,
        printer=printer,
        **kwargs
    ))
