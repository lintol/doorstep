import docker
from autobahn.wamp.exception import ApplicationError, SerializationError

from .encoders import Serializable

class LintolDoorstepException(Serializable, ApplicationError):
    def __init__(self, exception, processor=None, status_code=None, message=None):
        self.exception = exception
        self.processor = processor
        if message is not None:
            self._message = message
        if status_code is not None:
            self._status_code = status_code
        ApplicationError.__init__(self, str(self), self.status_code, **self.__serialize__())

    def __str__(self):
        return type(self).__name__

    @property
    def message(self):
        if hasattr(self, '_message'):
            return self._message
        return self.serialize_exception()

    @property
    def status_code(self):
        if hasattr(self, '_status_code'):
            return self._status_code
        if hasattr(self.exception, 'status_code'):
            return self.exception.status_code
        return None

    def serialize_exception(self):
        return str(self.exception)

    def __serialize__(self):
        return {
            'code': self.status_code,
            'processor': self.processor,
            'exception': type(self.exception).__name__,
            'message': self.message
        }

class LintolDoorstepContainerException(LintolDoorstepException):
    @property
    def status_code(self):
        if hasattr(self, '_status_code'):
            return self._status_code
        return self.exception.exit_status

    def serialize_exception(self):
        if self.exception.stderr:
            try:
                return self.exception.stderr.decode('utf-8')
            except:
                return '[could not encode stderr from container]'

        return str(self.exception)
