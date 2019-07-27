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

def set_config(config, key, value):
    keys = key.split('.')

    level = config
    for k in keys[:-1]:
        if k not in level:
            level[k] = {}
        level = level[k]

    level[-1] = value

def load_config():
    global _config

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

    return config

_mo = None
def load_from_minio(config, prefix, location):
    global _mo

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
        data_object = _mo.get_object(mo_bucket, location)
        stream_req = io.BytesIO()
        os.makedirs(os.path.join('/tmp', 'minio'), exist_ok=True)
        path = os.path.join('/tmp', 'minio', location)
        with open(path, 'rb') as f:
            for d in data_object.stream(32*1024):
                f.write(d)
    except MinioResponseError as err:
        raise err

    return path


def load_reference_data(config, location):
    path = os.path.join(os.path.dirname(__file__), '../../..', 'tests', 'examples', 'data', location)

    if 'reference-storage' in config:
        if 'storage' in config['reference-data'] and config['reference-data']['storage'] == 'minio':
            path = load_from_minio(config, config['reference-data']['prefix'], location)

    return path
