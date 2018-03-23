import json

class Serializable:
    def __serialize__(self):
        return str(self)

class DoorstepJSONEncoder(json.JSONEncoder):
    def default(o):
        if isinstance(o, JSONSerializable):
            return json.JSONEncoder(o.__serialize__())

        return json.JSONEncoder(o)

json_dumps = lambda *args, **kwargs: json.dumps(*args, **kwargs, cls=DoorstepJSONEncoder)
