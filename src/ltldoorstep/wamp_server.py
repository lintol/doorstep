from autobahn.asyncio.wamp import ApplicationRunner, ApplicationSession
import asyncio
import json
from autobahn.wamp.types import RegisterOptions
from collections import OrderedDict
from contextlib import contextmanager
import os
import uuid

class SessionSet(OrderedDict):
    def __init__(self, engine):
        self._engine = engine

    def add(self):
        session = self._engine.make_session()

        ssn = session.__enter__()
        ssn['__context__'] = session

        self[ssn['name']] = ssn

        return ssn

    def __enter__(self):
        return self

    def __exit__(self, exctyp, excval, exctbk):
        exceptions = []
        for session in self.values():
            try:
                session['__context__'].__exit__(exctyp, excval, exctbk)
            except Exception as e:
                exceptions.append(e)

        if exceptions:
            raise RuntimeError(exceptions)

def make_session_set(engine):
    sessions = SessionSet(engine)
    try:
        yield sessions
    finally:
        sessions.clear()

class ProcessorResource():
    def __init__(self, engine):
        self._engine = engine

    async def post(self, filename, content, session):
        module_name = os.path.splitext(os.path.basename(filename))[0]

        return self._engine.add_processor(module_name, content.encode('utf-8'), session)

class DataResource():
    def __init__(self, engine):
        self._engine = engine

    async def post(self, filename, content, session):
        return self._engine.add_data(filename, content.encode('utf-8'), session)

class ReportResource():
    def __init__(self, engine):
        self._engine = engine

    async def get(self, session):
        await session['monitor_output']

        return json.dumps(await self._engine.get_output(session))

class DoorstepComponent(ApplicationSession):
    def __init__(self, engine, sessions, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._id = 'ltlwc-%s' % str(uuid.uuid4())
        self._engine = engine
        self._sessions = sessions

        self._resource_processor = ProcessorResource(self._engine)
        self._resource_data = DataResource(self._engine)
        self._resource_report = ReportResource(self._engine)

    def get_session(self, name):
        return self._sessions[name]

    def make_session(self):
        return self._sessions.add()

    async def wrap_register(self, endpoint, callback):
        uri = 'com.ltldoorstep.{server}.{endpoint}'.format(server=self._id, endpoint=endpoint)

        async def _routine(session, *args, **kwargs):
            return await callback(*args, session=self.get_session(session), **kwargs)

        return await self.register(_routine, uri)

    async def onJoin(self, details):
        async def get_session_pair():
            session = self.make_session()

            # Kick off observer coro
            __, monitor_output = await self._engine.monitor_pipeline(session)
            monitor_output = asyncio.ensure_future(monitor_output)

            monitor_output.add_done_callback(lambda output: self.publish(
                'com.ltldoorstep.event_result',
                self._id,
                session['name']
            ))

            session['monitor_output'] = monitor_output

            return (self._id, session['name'])

        await self.register(
            get_session_pair,
            'com.ltldoorstep.engage',
            RegisterOptions(invoke='roundrobin')
        )
        await self.wrap_register('processor.post', self._resource_processor.post)
        await self.wrap_register('data.post', self._resource_data.post)
        await self.wrap_register('report.get', self._resource_report.get)


def launch_wamp(engine):
    runner = ApplicationRunner(url='ws://localhost:8080/ws', realm='realm1')

    with SessionSet(engine) as sessions:
        runner.run(lambda *args, **kwargs: DoorstepComponent(engine, sessions, *args, **kwargs))
