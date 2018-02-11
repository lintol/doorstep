"""Engine for running over a dask cluster."""

import shutil
import uuid
import os
import asyncio
import tornado
from contextlib import contextmanager
from dask.distributed import Client
from .dask_common import execute
from ..file import make_file_manager
import docker
import tempfile
import json
from .engine import Engine


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

    def add_processor(self, filename, content, metadata, session):
        if 'processors' not in session:
            session['processors'] = []

        session['processors'].append({
            'name' : str(uuid.uuid4()),
            'filename': filename,
            'content': content,
            'metadata': metadata
        })

    async def run(self, filename, workflow_module, metadata, bucket=None):
        """Start the execution process over the cluster."""

        with open(filename, 'r') as data_file:
            data_content = data_file.read()

        basename = os.path.basename(workflow_module)
        module_name = os.path.splitext(basename)[0]
        with open(workflow_module, 'r') as workflow_file:
            workflow_content = workflow_file.read()

        processors = [{
            'name': module_name,
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
            result = await self._run(data['filename'], data['content'], session['processors'], self.bind_ltldoorstep_module)
            session['result'] = result
            session['completion'].release()

        asyncio.ensure_future(run_when_ready())

        return (False, session['completion'].acquire())

    async def get_output(self, session):
        if 'result' in session:
            result = session['result']
        else:
            result = None

        return result

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
            if type(pc) == bytes:
                pc = pc.decode('utf-8')
            content[processor['filename']] = pc

        with tempfile.TemporaryDirectory('-doorstep-docker-engine-storage') as mounted_dir, make_file_manager(content=content) as file_manager:
            data_file = file_manager.get(data_filename)

            for processor in processors:
                processor_root = os.path.join(mounted_dir, 'processors', processor['name'])
                data_root = os.path.join(mounted_dir, 'data')
                out_root = os.path.join(mounted_dir, 'out')
                metadata = processor['metadata']

                os.makedirs(processor_root)
                os.makedirs(data_root)
                os.makedirs(os.path.join(out_root, 'raw'))

                with open(os.path.join(processor_root, 'metadata.json'), 'w') as metadata_file:
                    json.dump(metadata, metadata_file)

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
                    'LINTOL_INPUT_DATA': '/pfs/data'
                }
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
