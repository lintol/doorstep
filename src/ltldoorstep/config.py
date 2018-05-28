import configparser
import logging
import os
import json
import collections

def load_config():
    config = {
        'report': {
            'max-length-chars': 10000
        }
    }

    try:
        with open(os.path.join(os.path.expanduser("~"), '.ltldoorsteprc.yaml'), 'r') as config_file:
            config.update(yaml.load(config_file))
    except IOError:
        logging.info(_("No config file found"))

    return config
