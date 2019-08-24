from flask import current_app

import logging
from ltldoorstep.config import set_config, load_config
from ltldoorstep import config as lconfig
from ltldoorstep.location_utils import load_berlin
import json
import requests
from flask_restful import Resource, abort, reqparse
from ltldoorstep.file import make_file_manager
from ltldoorstep.engines import engines
from ltldoorstep.metadata import DoorstepContext
from ltldoorstep.config import load_config
from ltldoorstep.encoders import json_dumps
from ltldoorstep.errors import LintolDoorstepException
from ltldoorstep.engines.openfaas import ALLOWED_PROCESSORS, FUNCTION_CONTAINER_PREFIX

import asyncio
import os

ENGINE = 'dask.threaded'
ENGINE_CONFIG = {}
WORKFLOW = '/home/user/.local/lib/python3.6/site-packages/ltldoorstep_examples/dt_classify_category.py'


class Handler(Resource):
    _bucket = None
    _config = {}

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('filename', location='json')
        parser.add_argument('metadata', location='json')
        parser.add_argument('workflow', location='json')
        parser.add_argument('settings', location='json')
        args = parser.parse_args()

        engine = engines[ENGINE](config=self._config)

        metadata = args['metadata']
        if metadata is None:
            metadata = {}

        filename = args['filename']
        if filename is None:
            filename = ""

        workflow = args['workflow']
        if workflow not in ALLOWED_PROCESSORS:
            raise RuntimeError(_("Could not find {} in allowed processors for OpenFaaS engine.").format(workflow))

        function, path = ALLOWED_PROCESSORS[workflow]
        workflow = os.path.join(FUNCTION_CONTAINER_PREFIX, path)

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
        else:
            coro = engine.run(filename, workflow, metadata, bucket=self._bucket)

        try:
            report = loop.run_until_complete(coro)
        except Exception as e:
            if not isinstance(e, LintolDoorstepException):
                e = LintolDoorstepException(e)
            return {'error': 1, 'exception': json.loads(json_dumps(e))}

        result = report.compile(filename, metadata)

        return result

    @classmethod
    def preload(cls):
        cls._config = load_config()

        set_config('reference-data.storage', 'minio')
        set_config('storage.minio.region', 'us-east-1') # Fixed by Minio
        for k in ('bucket', 'key', 'secret', 'prefix', 'endpoint'):
            filename = os.path.join('/var', 'openfaas', 'secrets', f'minio_{k}')
            with open(filename, 'r') as f:
                value = f.read().strip()

            if k == 'prefix':
                set_config('reference-data.prefix', value)
            else:
                set_config(f'storage.minio.{k}', value)

        load_berlin()

        debug = cls._config['debug'] if 'debug' in cls._config else False
        logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
        cls.logger = logging.getLogger(__name__)

        return True
