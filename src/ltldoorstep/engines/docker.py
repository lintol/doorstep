"""Engine for running over a dask cluster."""

import shutil
import requests
import uuid
import os
import asyncio
from urllib.parse import urlparse
from contextlib import contextmanager
from dask.distributed import Client
from .dask_common import execute
from ..file import make_file_manager
import docker
import tempfile
import json
from .engine import Engine
from ..processor import compile_report, combine_reports


DEFAULT_CLIENT = 'tcp://localhost:8786'

class DockerEngine(Engine):
    """Allow execution on local docker containers."""

    client_url = DEFAULT_CLIENT
    bind_ltldoorstep_module = False

    def __init__(self, config=None):
        if config:
            if 'url' in config:
                self.client_url = config['url']
            if 'bind' in config and config['bind']:
                self.bind_ltldoorstep_module = True

    @staticmethod
    def description():
        return _("Run processor(s) locally via Docker")

    @staticmethod
    def config_help():
        return {
            'bind': _("Useful for debugging ltldoorstep itself,\n" +
                "bind-mounts the ltldoorstep module into the executing container")
        }

    def add_data(self, filename, content, session):
        data = {
            'filename': filename,
            'content': content
        }
        asyncio.ensure_future(session['queue'].put(data))

    def add_processor(self, modules, metadata, session):
        """Mark a module_name as a processor."""

        lang = 'C.UTF-8' # TODO: more sensible default

        if 'lang' in metadata:
            # TODO: check lang is valid
            lang = metadata['lang']

        if 'processors' not in session:
            session['processors'] = []

        for uid, processor in metadata['definitions'].items():
            docker_image = 'lintol/doorstep'
            docker_revision = 'latest'

            if 'docker' in processor:
                if 'image' in processor['docker']:
                    docker_image = processor['docker']['image']
                    docker_revision = processor['docker']['revision']

            docker = '{image}:{revision}'.format(image=docker_image, revision=docker_revision)
            configuration = {
                'name': uid,
                'definition': processor['definition'] if 'definition' in processor else {},
                'configuration': processor['configuration'] if 'configuration' in processor else {},
                'settings': processor['settings'] if 'settings' in processor else {},
                'supplementary': processor['supplementary'] if 'supplementary' in processor else {}
            }

            filename = None
            content = None
            if 'module' in processor:
                filename = processor['module']
                if processor['module'] in modules:
                    content = modules[processor['module']]
                else:
                    raise RuntimeError(_("Module content missing from processor(s)"))

            session['processors'].append({
                'name' : uid,
                'filename': filename,
                'content': content,
                'metadata': configuration
            })

    async def run(self, filename, workflow_module, metadata, bucket=None):
        """Start the execution process over the cluster."""

        with open(filename, 'r') as data_file:
            data_content = data_file.read()

        basename = os.path.basename(workflow_module)
        with open(workflow_module, 'r') as workflow_file:
            workflow_content = workflow_file.read()

        processors = [{
            'name': 'processor',
            'metadata': metadata,
            'filename': basename,
            'content': workflow_content
        }]

        return await self._run(filename, data_content, processors, self.bind_ltldoorstep_module)

    async def monitor_pipeline(self, session):
        loop = asyncio.get_event_loop()

        session['completion'] = asyncio.Lock()

        async def run_when_ready():
            session['completion'].acquire()
            data = await session['queue'].get()
            try:
                result = await self._run(data['filename'], data['content'], session['processors'], self.bind_ltldoorstep_module)
                session['result'] = result
            finally:
                session['completion'].release()

        asyncio.ensure_future(run_when_ready())

        return (False, session['completion'].acquire())

    @staticmethod
    def combine_reports(data_file, reports):
        base = compile_report(data_file, None)
        for report in reports:
            base = combine_reports(report, base)
        return base

    async def get_output(self, session):
        await session['completion'].acquire()

        result = session['result']

        session['completion'].release()

        return json.dumps(result)

    @staticmethod
    async def _run(data_filename, data_content, processors, bind_ltldoorstep_module):
        """Start the execution process over the cluster for a given client."""

        result = None

        filename = data_filename
        if type(data_content) == bytes:
            data_content = data_content.decode('utf-8')

        content = {
            filename: data_content
        }
        for processor in processors:
            pc = processor['content']
            if pc and processor['filename']:
                if type(pc) == bytes:
                    pc = pc.decode('utf-8')
                content[processor['filename']] = pc

        with tempfile.TemporaryDirectory('-doorstep-docker-engine-storage') as mounted_dir, make_file_manager(content=content) as file_manager:
            data_file = file_manager.get(data_filename)

            report_files = []
            for processor in processors:
                processor_root = os.path.join(mounted_dir, 'processors', processor['name'])
                data_root = os.path.join(mounted_dir, 'data')
                out_root = os.path.join(mounted_dir, 'out')
                metadata = processor['metadata']

                os.makedirs(processor_root)
                os.makedirs(data_root)
                os.makedirs(os.path.join(out_root, 'raw'))

                if 'supplementary' in metadata:
                    supplementary_internal = {}
                    for i, (key, supplementary) in enumerate(metadata['supplementary'].items()):
                        error = _("(unknown error)")
                        if supplementary.startswith('error://'):
                            error = supplementary[len('error://'):]
                            raise RuntimeError(_("Supplementary data could not be retrieved: %s") % error)

                        supplementary_basename = 's%d-%s' % (i, os.path.basename(urlparse(supplementary).path))

                        r = requests.get(supplementary, stream=True)
                        if r.status_code != 200:
                            raise RuntimeError(_("Could not retrieve supplementary data: %d" % r.status_code))

                        print(_("Downloading %s") % supplementary)
                        with open(os.path.join(processor_root, supplementary_basename), 'wb') as supplementary_file:
                            for chunk in r.iter_content(chunk_size=1024):
                                supplementary_file.write(chunk)

                        supplementary_internal[key] = {
                            'source': supplementary,
                            'location': os.path.join('/pfs', 'processors', processor['name'], supplementary_basename)
                        }

                    metadata['supplementary'] = supplementary_internal

                with open(os.path.join(processor_root, 'metadata.json'), 'w') as metadata_file:
                    json.dump(metadata, metadata_file)

                if processor['filename']:
                    shutil.copy(file_manager.get(processor['filename']), os.path.join(processor_root, processor['filename']))
                data_basename = os.path.basename(data_filename)
                shutil.copy(data_file, os.path.join(data_root, data_basename))

                mounts = [('mounted_dir', '/pfs')]

                docker_image = 'lintol/doorstep'
                docker_revision = 'latest'
                lang = 'C.UTF-8' # TODO: more sensible default

                if 'docker' in metadata:
                    if 'image' in metadata['docker']:
                        docker_image = metadata['docker']['image']
                        docker_revision = metadata['docker']['revision']

                if 'lang' in metadata:
                    # TODO: check lang is valid
                    lang = metadata['lang']

                envs = {
                    'LANG': lang,
                    'LINTOL_PROCESSOR_DIRECTORY': '/pfs/processors/%s' % processor['name'],
                    'LINTOL_OUTPUT_FILE': '/pfs/out/raw/%s.json' % processor['name'],
                    'LINTOL_METADATA': '/pfs/processors/%s/metadata.json' % processor['name'],
                    'LINTOL_INPUT_DATA': '/pfs/data',
                    'LINTOL_DATA_FILE': data_basename
                }
                report_files.append(os.path.join(out_root, 'raw', '%s.json' % processor['name']))

                client = docker.from_env()
                mounts = [
                    docker.types.Mount('/pfs', mounted_dir, type='bind')
                ]

                if bind_ltldoorstep_module:
                    ltldoorstep_root_dir = os.path.join(
                        os.path.dirname(__file__),
                        '..',
                        '..',
                        '..'
                    )
                    mounts.append(docker.types.Mount(
                        '/doorstep',
                        ltldoorstep_root_dir,
                        type='bind'
                    ))

                client.containers.run(
                    '%s:%s' % (docker_image, docker_revision),
                    environment=envs,
                    mounts=mounts,
                    user=1000,
                    network_mode='none',
                    cap_drop='ALL'
                )

            reports = []
            for report_file in report_files:
                with open(report_file, 'r') as report_f:
                    reports.append(json.load(report_f))

            result = DockerEngine.combine_reports(data_basename, reports)

        return result

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