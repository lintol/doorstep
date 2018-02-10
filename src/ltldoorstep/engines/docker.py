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

    def config_help(self):
        return {
            'bind': _("Useful for debugging ltldoorstep itself,\n" +
                "bind-mounts the ltldoorstep module into the executing container")
        }

    def add_data(self, filename, content, metadata, session):
        session['data-filename'] = filename
        session['data-content'] = content

    def add_processor(self, filename, content, session):
        if 'processors' in session['processors']:
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

        return await self._run(filename, metadata, data_content, processors, workflow_content, self.bind_ltldoorstep_module)

    async def monitor_pipeline(self, session):
        result = await self._run(session['data-filename'], session['metadata'], session['data-content'], session['processors'], self.bind_ltldoorstep_module)

        return result

    @staticmethod
    async def _run(data_filename, metadata, data_content, processors, workflow_content, bind_ltldoorstep_module):
        """Start the execution process over the cluster for a given client."""

        result = None

        metadata = metadata
        filename = data_filename
        content = {
            filename: data_content
        }
        for processor in processors:
            content[processor['filename']] = workflow_content

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
                print(file_manager.get(processor['filename']), os.path.join(processor_root, processor['filename']))
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
                    print(ltldoorstep_root_dir)
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
        data_name = '%s-data' % name
        processors_name = '%s-processors' % name

        session = {
            'name': name,
            'data': data_name,
            'processors': processors_name
        }

        yield session
