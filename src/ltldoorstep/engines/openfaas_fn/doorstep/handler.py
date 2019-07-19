from flask import current_app
import json
import requests
from flask_restful import Resource, abort, reqparse
from ltldoorstep.file import make_file_manager
from ltldoorstep.engines import engines
from ltldoorstep.metadata import DoorstepContext
from ltldoorstep.config import load_config

import asyncio
import os

ENGINE = 'dask.threaded'
ENGINE_CONFIG = {}
WORKFLOW = '/home/user/.local/lib/python3.6/site-packages/ltldoorstep_examples/dt_classify_category.py'


class Handler(Resource):
    _bucket = None

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('filename', location='json')
        parser.add_argument('metadata', location='json')
        parser.add_argument('workflow', location='json')
        parser.add_argument('settings', location='json')
        args = parser.parse_args()

        engine = engines[ENGINE](config=ENGINE_CONFIG)

        metadata = args['metadata']
        if metadata is None:
            metadata = {}

        filename = args['filename']
        if filename is None:
            filename = ""

        workflow = args['workflow']
        if workflow is None:
            workflow = ""

        metadata = DoorstepContext.from_dict(json.loads(metadata))

        loop = asyncio.get_event_loop()

        if filename.startswith('https:') or filename.startswith('http:'):
            r = requests.get(filename)

            with open(workflow, 'r') as f:
                processors = [{
                    'name' : filename,
                    'filename': workflow,
                    'content': f.read(),
                    'metadata': metadata
                }]

            coro = engine.run_with_content(filename, r.text, processors)
            report = loop.run_until_complete(coro)
            result = report.compile(filename, metadata)
        else:
            coro = engine.run(filename, workflow, metadata, bucket=self._bucket)
            result = loop.run_until_complete(coro)

        return result

    @classmethod
    def preload(cls):
        if 'MINIO_BUCKET' in os.environ:
            self._bucket = os.environ['MINIO_BUCKET']

        if 'LINTOL_ENGINE' in os.environ:
            engine = os.environ['LINTOL_ENGINE']
        else:
            engine = 'dask.threaded'

        if 'LINTOL_PROCESSOR_DIRECTORY' in os.environ:
            workflow = None

            for f in os.listdir(os.environ['LINTOL_PROCESSOR_DIRECTORY']):
                if f.endswith('.py'):
                    workflow = f
                    break

            if not workflow:
                raise RuntimeError("No processor directory found: LINTOL_PROCESSOR_DIRECTORY has no python file")
        else:
            raise RuntimeError("No processor directory found: LINTOL_PROCESSOR_DIRECTORY must be set")

        logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
        cls.logger = logging.getLogger(__name__)
        cls.workflow = workfloW

    @classmethod
    def preload(cls):
        print("!!!")
        return True
