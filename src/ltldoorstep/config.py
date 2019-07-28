import configparser
import io
import logging
import os
import json
import collections
import yaml

try:
    from minio import Minio
    from minio.error import ResponseError as MinioResponseError
except ImportError:
    Minio = None
    MinioResponseError = RuntimeError

_config = {}
_active_config = {}

def set_config(key, value):
    global _active_config

    keys = key.split('.')

    level = config
    for k in keys[:-1]:
        if k not in level:
            level[k] = {}
        level = level[k]

    level[-1] = value

def load_config():
    global _config, _active_config

    config = {
        'report': {
            'max-length-chars': 500000
        }
    }

    try:
        with open(os.path.join(os.path.expanduser("~"), '.ltldoorsteprc.yaml'), 'r') as config_file:
            config.update(yaml.load(config_file, Loader=yaml.SafeLoader))
    except IOError:
        logging.info(_("No config file found"))

    config.update(_config)
    _active_config = config

    return config

_mo = None
def load_from_minio(prefix, location):
    global _mo, _active_config
    config = _active_config

    path = os.path.join('/tmp', 'minio', location)
    if os.path.exists(path):
        return path

    if _mo is None:
        _mo = Minio(
            config['storage']['minio']['endpoint'],
            access_key=config['storage']['minio']['key'],
            secret_key=config['storage']['minio']['secret'],
            region=config['storage']['minio']['region'],
            secure=True
        )
    mo_bucket = config['storage']['minio']['bucket']

    try:
        print(location, mo_bucket)
        print(config['storage']['minio'])
        data_object = _mo.get_object(mo_bucket, f'{prefix}/{location}')
        stream_req = io.BytesIO()
        os.makedirs(os.path.join('/tmp', 'minio'), exist_ok=True)
        with open(path, 'wb') as f:
            for d in data_object.stream(32*1024):
                f.write(d)
    except MinioResponseError as err:
        raise err

    return path


def load_reference_data(location):
    global _active_config
    config = _active_config

    path = os.path.join(os.path.dirname(__file__), '../../..', 'tests', 'examples', 'data', location)

    if 'reference-data' in config:
        if 'storage' in config['reference-data'] and config['reference-data']['storage'] == 'minio':
            path = load_from_minio(config['reference-data']['prefix'], location)

    return path
