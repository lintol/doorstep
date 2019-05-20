"""Engine for running a job, using dask, within this process."""

import uuid
import logging
from contextlib import contextmanager
from importlib.machinery import SourceFileLoader
from ..errors import LintolDoorstepException, LintolDoorstepContainerException
from ..reports.report import combine_reports
from ..file import make_file_manager
from ..encoders import json_dumps
from .dask_common import run as dask_run
from .engine import Engine
import asyncio
from asyncio import Lock, ensure_future, Queue
from ..ini import DoorstepIni


class DaskThreadedEngine(Engine):
    """Allow execution of a dask workflow within this process."""

    def add_data(self, filename, content, redirect, session):
        data = {
            'filename': filename,
            'content': content
        }
        # logging.warn("Data added")
        ensure_future(session['queue'].put(data))

    def add_processor(self, modules, ini, session):

        if 'processors' not in session:
            session['processors'] = []

        if type(ini) is dict:
            ini = DoorstepIni.from_dict(ini)

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

        logging.warn("Processor added")

    async def monitor_pipeline(self, session):
        logging.warn("Waiting for processor and data")

        session['completion'] = Lock()

        # currently crashing here for a large dataset....
        async def run_when_ready():
            session['completion'].acquire()
            # error here
            # session doesn't seem to be yielding before it crashes
            data = await session['queue'].get()
            logging.warn('Session var here after data is set %s' % session['queue'])
            if data == 0:
                logging.warn("Releasing session")
                session['completion'].release()
            try:
                result = await self.run_with_content(data['filename'], data['content'], session['processors'])
                session['result'] = result
            except Exception as error:
                if not isinstance(error, LintolDoorstepException):
                    error = LintolDoorstepException(error)
                session['result'] = error
            finally:
                session['completion'].release()
                logging.warn("EXCEPTION HERE?************")
        ensure_future(run_when_ready())

        return (False, session['completion'].acquire())

    async def get_output(self, session):
        await session['completion'].acquire()

<<<<<<< HEAD
        # print(session)
=======
>>>>>>> e2230e70be4d2a0f970631db26b09a607d13d563
        result = session['result']

        session['completion'].release()

        if isinstance(result, LintolDoorstepException):
            raise result

        return result

    @staticmethod
    async def run_with_content(filename, content, processors):
        reports = []
        if type(content) == bytes:
            content = content.decode('utf-8')

        for processor in processors:
            workflow_module = processor['content']
            if type(workflow_module) == bytes:
                workflow_module = workflow_module.decode('utf-8')

            metadata = processor['metadata']
<<<<<<< HEAD
            # print(processor, 'B')
            with make_file_manager(content={filename: content, processor['filename']: workflow_module}) as file_manager:
                # print(file_manager.get(processor['filename']), processor['filename'], workflow_module, 'A')
=======
            with make_file_manager(content={filename: content, processor['filename']: workflow_module}) as file_manager:
>>>>>>> e2230e70be4d2a0f970631db26b09a607d13d563
                mod = SourceFileLoader('custom_processor', file_manager.get(processor['filename']))
                local_file = file_manager.get(filename)
                report = dask_run(local_file, mod.load_module(), metadata, compiled=False)
                reports.append(report)
        report = combine_reports(*reports)

        return report.compile(filename, {})

    @staticmethod
    async def run(filename, workflow_module, metadata, bucket=None):
        """Start the multi-threaded execution process."""

        mod = SourceFileLoader('custom_processor', workflow_module)
<<<<<<< HEAD
        try:
            result = None
            with make_file_manager(bucket) as file_manager:
                local_file = file_manager.get(filename)
                # print('RUN')
                result = dask_run(local_file, mod.load_module(), metadata)
            return result
        except:
            return('Arrrrrrrghhhhhhhhhhhhhhhhh')
=======

        result = None
        with make_file_manager(bucket) as file_manager:
            local_file = file_manager.get(filename)
            result = dask_run(local_file, mod.load_module(), metadata)

        return result
>>>>>>> e2230e70be4d2a0f970631db26b09a607d13d563

    @contextmanager
    def make_session(self):
        """Set up a workflow session.

        This creates a self-contained set of dask constructs representing our operation.
        """

        name = 'doorstep-%s' % str(uuid.uuid4())
        data_name = '%s-data' % name

        session = {
            'name': name,
            'data': data_name,
            'queue': Queue()
        }
        logging.warn("Yeiling session - %s " % session)
        yield session
