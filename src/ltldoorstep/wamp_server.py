from autobahn.asyncio.wamp import ApplicationRunner, ApplicationSession
import sys
import traceback
import time
import requests
import docker
from urllib.parse import urlparse
import asyncio
import json
import logging
from autobahn.wamp.types import RegisterOptions
from collections import OrderedDict
from contextlib import contextmanager
import os
import uuid
from .ini import DoorstepIni
from .errors import LintolDoorstepException

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
    def __init__(self, engine, config):
        self._engine = engine
        self._config = config

    async def post(self, modules, ini, session):
        processors = {}
        for module, content in modules.items():
            processors[module] = content.encode('utf-8')

        ini = DoorstepIni.from_dict(ini)
        logging.warn(ini)

        return self._engine.add_processor(processors, ini, session)

class DataResource():
    def __init__(self, engine, config):
        self._engine = engine
        self._config = config

    async def post(self, filename, content, redirect, session=None):
        logging.warn(_("Data posted"))
        logging.warn(redirect)
        if redirect:
            if content.startswith('file://'):
                with open(content[len('file://'):], 'r') as file_obj:
                    content = file_obj.read()
            else:
                r = requests.get(content, stream=True)
                if r.status_code != 200:
                    raise RuntimeError(_("Could not retrieve supplementary data: %d") % r.status_code)

                logging.warn(_("Downloading %s") % content)
                content = ''
                for chunk in r.iter_content(chunk_size=1024):
                    content += chunk.decode('utf-8')

        return self._engine.add_data(filename, content.encode('utf-8'), redirect, session)

class ReportResource():
    def __init__(self, engine, config):
        self._engine = engine
        self._config = config

    async def get(self, session):
        await session['monitor_output']

        results = await self._engine.get_output(session)
        result_string = json.dumps(results)

        if len(result_string) > self._config['report']['max-length-chars']:
            raise RuntimeError(_("Report is too long: %d characters") % len(results))

        return result_string

class DoorstepComponent(ApplicationSession):
    def __init__(self, engine, sessions, config, debug, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._id = 'ltlwc-%s' % str(uuid.uuid4())
        self._engine = engine
        self._sessions = sessions
        self._config = config
        self._debug = debug

        self._resource_processor = ProcessorResource(self._engine, self._config)
        self._resource_data = DataResource(self._engine, self._config)
        self._resource_report = ReportResource(self._engine, self._config)

    def get_session(self, name):
        return self._sessions[name]

    def make_session(self):
        return self._sessions.add()

    async def wrap_register(self, endpoint, callback):
        uri = 'com.ltldoorstep.{server}.{endpoint}'.format(server=self._id, endpoint=endpoint)

        async def _routine(session, *args, **kwargs):
            try:
                result = await callback(*args, session=self.get_session(session), **kwargs)
            except LintolDoorstepException as e:
                if self._debug:
                    logging.error(e.processor)
                    logging.error(e.exception)
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_tb(exc_traceback)
                raise e

            return result

        return await self.register(_routine, uri)

    async def onJoin(self, details):
        print("Joining")
        async def get_session_pair():
            session = self.make_session()
            print(_("Engaging for session %s") % session['name'])

            # Kick off observer coro
            try:
                __, monitor_output = await self._engine.monitor_pipeline(session)
                monitor_output = asyncio.ensure_future(monitor_output)
            except Exception as e:
                print("ERRORROROROR", e)
                session['monitor_output'] = asyncio.sleep(1.0)
                return (self._id, session['name'])
            def output_results(output):
                logging.warn('outputting')
                return self.publish(
                    'com.ltldoorstep.event_result',
                    self._id,
                    session['name']
                )

            monitor_output.add_done_callback(output_results)

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

    def onDisconnect(self):
        logging.error(_("Disconnected from WAMP router"))
        asyncio.get_event_loop().stop()


def launch_wamp_real(engine, router='localhost:8080', config={}, debug=False):
    runner = ApplicationRunner(url=('ws://%s/ws' % router), realm='realm1')

    with SessionSet(engine) as sessions:
        print("Running")
        runner.run(lambda *args, **kwargs: DoorstepComponent(engine, sessions, config, debug, *args, **kwargs))
        print("Run")

def launch_wamp(engine, router='#localhost:8080', config={}, debug=False):
    if router[0] == '#':
        router= router[1:]
        fallback = True
    else:
        fallback = False

    try:
        launch_wamp_real(engine, router, config, debug)
    except ConnectionRefusedError as e:
        if fallback:
            c = None
            try:
                print("No current WAMP router, starting one")
                client = docker.from_env()

                hostname, port = router.split(':')
                c = client.containers.run(
                    'crossbario/crossbar',
                    ports={
                        '8080/tcp': (hostname, port if port else 8080)
                    },
                    detach=True,
                    remove=True
                )
                time.sleep(3)
                launch_wamp_real(engine, router, config, debug)
            finally:
                if c:
                    print("[cleaning purpose-run WAMP router]")
                    c.stop()
        else:
            print("No current WAMP router found (tip: if you add a # at the front of the router URL, we'll try and start it for you).")
