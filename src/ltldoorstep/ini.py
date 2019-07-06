import json
import logging
from .metadata import DoorstepContext

class DoorstepIni:
    def __init__(self, lang=None, definitions={}, context_package=None):
        self.lang = lang
        self._context_package = context_package
        self.definitions = definitions

    def __repr__(self):
        return "<DoorstepIni: {}>".format(self.to_dict())

    def has_package(self):
        return bool(self._context_package)

    @property
    def package(self):
        package = self._context_package

        if type(package) is str:
            package = json.loads(package)
            self._context_package = package

        return package

    @package.setter
    def package(self, package):
        self._context_package = package

    @classmethod
    def from_dict(cls, dct):
        kwargs = {}
        context = {}

        if 'context' in dct:
            if 'package' in dct['context']:
                kwargs['context_package'] = dct['context']['package']
                context['context'] = dct['context']

        if 'lang' in dct:
            kwargs['lang'] = dct['lang']
            context['lang'] = dct['lang']

        if 'definitions' in dct:
            kwargs['definitions'] = {}
            for d, processor in dct['definitions'].items():
                context.update(processor)
                kwargs['definitions'][d] = DoorstepContext.from_dict(context)

        return cls(**kwargs)

    def to_dict(self):
        package = self._context_package
        if type(package) is not str:
            package = json.dumps(package)

        return {
            'lang': self.lang,
            'context': {
                'package': package
            },
            'definitions': {d: metadata.to_dict() for d, metadata in self.definitions.items()},
        }
