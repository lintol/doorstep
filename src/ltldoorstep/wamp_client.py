from autobahn.asyncio.wamp import ApplicationRunner, ApplicationSession
import asyncio

class WampClientComponent(ApplicationSession):
    def __init__(self, filename, workflow, *args, printer=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = filename
        self._workflow = workflow
        self._printer = printer

    async def onJoin(self, details):
        filename = self._filename
        workflow = self._workflow

        with open(filename, 'r') as file_obj:
            content = file_obj.read()

        with open(workflow, 'r') as file_obj:
            module = file_obj.read()

        self._server, self._session = await self.call('com.ltldoorstep.engage')

        await self.call_server('processor.post', filename, content)
        await self.call_server('data.post', workflow, module)
        await self.call_server('report.get')
        result = await self.call_server('report.get')

        if self._printer:
            self._printer.print_report(result)

        loop = asyncio.get_event_loop()
        loop.stop()

    async def call_server(self, endpoint, *args, **kwargs):
        return await self.call(
            'com.ltldoorstep.{server}.{endpoint}'.format(server=self._server, endpoint=endpoint),
            self._session,
            *args,
            **kwargs
        )


def launch_wamp(filename, workflow, printer):
    runner = ApplicationRunner(url='ws://localhost:8080/ws', realm='realm1')
    runner.run(lambda *args, **kwargs: WampClientComponent(filename, workflow, *args, printer=printer, **kwargs))
