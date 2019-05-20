import os
import requests
import time
import uuid
import json
import json
import logging
from autobahn.wamp.exception import ApplicationError
from autobahn.asyncio.wamp import ApplicationRunner, ApplicationSession
import asyncio

from .wamp_server import DoorstepComponent, SessionSet
from .ini import DoorstepIni
from .metadata import DoorstepContext

class WampClientComponent(ApplicationSession):
    """Connector to join and execute a WAMP session."""

    _server = None
    _session = None

    def __init__(self, filename, workflow, ini, *args, printer=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = filename
        self._workflow = workflow
        self._ini = ini
        self._printer = printer

    async def onJoin(self, details):
        """When we join the server, execute the client workflow."""

        with open(self._filename, 'r') as file_obj:
            content = file_obj.read()
        filename = os.path.basename(self._filename)

        with open(self._workflow, 'r') as file_obj:
            module = file_obj.read()
        workflow = os.path.basename(self._workflow)

        definitions = {
            str(uuid.uuid4()): DoorstepMetadata.from_dict({
                'module': workflow
            })
        }
        if not self._ini:
            self._ini = DoorstepIni(definitions=definitions)

        if type(self._ini) is str:
            with open(self._ini, 'r') as file_obj:
                self._ini = DoorstepIni.from_dict(json.load(file_obj))
        elif not self._ini.definitions:
            self._ini.definitions = definitions

        ini = self._ini

        self._server, self._session = await self.call('com.ltldoorstep.engage')

        print('C', ini.to_dict()['definitions'])
        await self.call_server('processor.post', {workflow: module}, ini.to_dict())
        await self.call_server('data.post', filename, content, False)

        try:
            result = json.loads(await self.call_server('report.get'))
        except ApplicationError as e:
            logging.error(e)
            result = None

        if self._printer and result:
            self._printer.build_report(result)

        # Stop and return control
        loop = asyncio.get_event_loop()
        loop.stop()

        #RMV
        #self._printer.print_output()

    async def call_server(self, endpoint, *args, **kwargs):
        """Generate the correct endpoint for the known server."""

        return await self.call(
            'com.ltldoorstep.{server}.{endpoint}'.format(server=self._server, endpoint=endpoint),
            self._session,
            *args,
            **kwargs
        )


def launch_wamp_real(router_url, filename, workflow, printer, ini):
    runner = ApplicationRunner(url=router_url, realm='realm1')
    runner.run(lambda *args, **kwargs: WampClientComponent(
        filename,
        workflow,
        ini,
        *args,
        printer=printer,
        **kwargs
    ))

def launch_wamp(router_url, filename, workflow, printer, ini):
    """Run the workflow against a WAMP server."""

    if router_url[0] == '#':
        router_url = router_url[1:]
        fallback = True
    else:
        fallback = False

    try:
        launch_wamp_real(router_url, filename, workflow, printer, ini)
    except:
        if fallback:
            input("Could not find router, pausing until you start one... (press Return to continue)")
            launch_wamp_real(router_url, filename, workflow, printer, ini)
