import json
from .metadata import DoorstepMetadata

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

        if 'context' in dct:
            if 'package' in dct['context']:
                kwargs['context_package'] = dct['context']['package']
                context = dct['context']
        else:
            context = False

        if 'definitions' in dct:
            kwargs['definitions'] = {}
            for d, processor in dct['definitions'].items():
                kwargs['definitions'][d] = DoorstepMetadata.from_dict(processor)

        if 'lang' in dct:
            kwargs['lang'] = dct['lang']

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
