"""Engine for running a job on an OpenFaaS cluster."""

from contextlib import contextmanager
import traceback
import requests
from requests.auth import HTTPBasicAuth
import asyncio
import time
import os
import sys
import json
import uuid
import jinja2
import logging
from ..ini import DoorstepIni
from ..errors import LintolDoorstepException, LintolDoorstepContainerException
from concurrent.futures import ThreadPoolExecutor
from .engine import Engine
from ..reports.report import Report, combine_reports

OPENFAAS_HOST = 'http://127.0.0.1:8084'
FUNCTION_CONTAINER_PREFIX = '/home/user/.local/lib/python3.6/site-packages/'
ALLOWED_PROCESSORS = {
    'datatimes/dt-classify-category:1': ('doorstep', 'ltldoorstep_examples/dt_classify_category.py'),
    'datatimes/dt-classify-location:1': ('doorstep', 'ltldoorstep_examples/dt_classify_location.py'),
    'datatimes/dt-comprehender:1': ('doorstep', 'ltldoorstep_examples/dt_comprehender.py')
}

class OpenFaaSEngine(Engine):
    """Allow execution of workflows on a OpenFaaS cluster."""

    def __init__(self, config=None):
        if config and 'engine' in config:
            config = config['engine']

            self.openfaas_host = OPENFAAS_HOST
            self.openfaas_cred = ''

            if 'openfaas' in config:
                config = config['openfaas']
                if 'host' in config:
                    self.openfaas_host = config['host']

                if 'credential' in config:
                    self.openfaas_cred = config['credential']

    @staticmethod
    def description():
        return '(not provided)'

    @staticmethod
    def config_help():
        return None

    def add_data(self, filename, content, redirect, session):
        data = {
            'filename': filename,
            'content': content
        }
        asyncio.ensure_future(session['queue'].put(data))

    def add_processor(self, modules, ini, session):
        """Mark a module_name as a processor."""

        lang = 'C.UTF-8' # TODO: more sensible default

        if type(ini) is dict:
            ini = DoorstepIni.from_dict(ini)

        if ini.lang:
            # TODO: check lang is valid
            lang = ini.lang

        if 'processors' not in session:
            session['processors'] = []

        for uid, metadata in ini.definitions.items():
            filename = None
            content = None
            if metadata.module:
                filename = metadata.module
                if metadata.module in modules:
                    content = modules[metadata.module]
                else:
                    error_msg = _("Module content missing from processor %s") % metadata.module
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)

            session['processors'].append({
                'name' : uid,
                'filename': filename,
                'content': content,
                'metadata': metadata
            })

    async def run(self, filename, workflow_module, metadata, bucket=None):
        """Start the execution process over the cluster."""

        processors = [{
            'name': workflow_module,
            'metadata': metadata,
            'filename': filename,
            'content': filename
        }]
        report = await self._run(filename, filename, processors, self.openfaas_host, self.openfaas_cred)
        return report.compile(filename, metadata)

    async def monitor_pipeline(self, session):
        session['completion'] = asyncio.Event()

        async def run_when_ready():
            # await session['completion'].acquire()
            data = await session['queue'].get()
            try:
                result = await self._run(data['filename'], data['content'], session['processors'], self.openfaas_host, self.openfaas_cred)
                session['result'] = result
            except Exception as error:
                __, __, exc_traceback = sys.exc_info()
                traceback.print_tb(exc_traceback)
                if not isinstance(error, LintolDoorstepException):
                    error = LintolDoorstepException(error)
                session['result'] = error
            finally:
                session['completion'].set()

        asyncio.ensure_future(run_when_ready())

        return (False, session['completion'].wait())

    async def get_output(self, session):
        await session['completion'].wait()

        result = session['result']

        if isinstance(result, LintolDoorstepException):
            raise result

        return result

    @staticmethod
    async def _run(filename, content, processors, openfaas_host, openfaas_cred):
        reports = []
        for processor in processors:
            metadata = processor['metadata']
            if metadata.tag in ALLOWED_PROCESSORS:
                tag = metadata.tag
            elif processor['name'] in ALLOWED_PROCESSORS:
                tag = processor['name']
            else:
                raise RuntimeError(_("Could not find {} or {} in allowed processors for OpenFaaS engine.").format(metadata.tag, processor['name']))
            function, path = ALLOWED_PROCESSORS[tag]

            try:
                rq = requests.post(f'{openfaas_host}/function/{function}', json={
                    'filename': content,
                    'workflow': tag,
                    'metadata': json.dumps(metadata.to_dict()),
                }, auth=HTTPBasicAuth('admin', openfaas_cred))
            except Exception as e:
                logging.error(e)

            logging.error(rq.content)

            report = Report.parse(rq.json())
            reports.append(report)

        report = combine_reports(*reports)

        return report

    @contextmanager
    def make_session(self):
        """Set up a workflow session.

        This creates a self-contained set of dask constructs representing our operation.
        """

        name = 'doorstep-%s' % str(uuid.uuid4())

        session = {
            'name': name,
            'queue': asyncio.Queue()
        }

        yield session
