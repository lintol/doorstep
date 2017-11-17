"""Exceptions occuring in the pipeline itself."""

class JobException(Exception):
    """Exception in executing a Pachyderm Pipeline."""

    _job = None
    _state = None
    _logs = None
    _default_message = "Job error"

    def __init__(self, message=None, job=None, logs=None):
        if not message:
            message = _(self._default_message)

        super().__init__(message)

        self._job = job
        self._logs = logs

        if job:
            self._state = job.get_state()

    def job(self):
        return self._job

    def state(self):
        return self._state

    def logs(self):
        return self._logs


class JobFailedException(JobException):
    """Exception in executing a Pachyderm Pipeline."""

    _default_message = "Job failed"

class JobKilledException(JobException):
    """Exception in executing a Pachyderm Pipeline."""

    _default_message = "Job was killed"
