"""Proxy class for a Pachyderm Pipeline."""

import time
from contextlib import contextmanager
from .repo import PachydermRepo

class PachydermPipeline:
    """Proxy class for a Pachyderm Pipeline."""

    _current_job = None

    def __init__(self, clients, definition):
        self._pipeline = definition['pipeline']
        self._clients = clients
        self._definition = definition
        self._output_repo = PachydermRepo(clients, self.get_name())

    def get_name(self):
        """Name of this pipeline."""

        return self._pipeline['name']

    def list_jobs(self):
        """Find jobs related to this pipeline."""

        return self._clients['pps'].list_job(self._pipeline).job_info

    def pull_output(self, path):
        """Pull a file from the output repo."""

        return self._output_repo.pull_file(path)

    def check_started(self):
        """Check whether started.

        Indicate whether we have started a run, and return job if so.
        """

        job_info = self.list_jobs()

        if job_info:
            # TODO: There should only be one job, but check
            return job_info[0]

        return False

    def wait_for_run(self, retry_count, ignore_states=(), tick_callback=None, error_suffix=""):
        """Wait until the this pipeline runs to a certain point."""

        job = self._current_job

        for __ in range(retry_count):
            if job and not job.state in ignore_states:
                break

            if tick_callback:
                tick_callback()

            job = self.check_started()
        else:
            raise RuntimeError(_("Gave up waiting for pipeline job%s") % error_suffix)

        self._current_job = job

        print()

        return job

@contextmanager
def make_pipeline(clients, definition, session):
    """Create a temporary pipeline for running session processors on session data."""

    definition['pipeline']['name'] = session['name']
    definition['input']['cross'][0]['atom']['repo'] = session['processors'].get_name()
    definition['input']['cross'][1]['atom']['repo'] = session['data'].get_name()

    clients['pps'].create_pipeline(**definition)
    pipeline = PachydermPipeline(clients, definition)
    try:
        yield pipeline
    finally:
        clients['pps'].delete_pipeline(pipeline.get_name(), delete_jobs=True, delete_repo=True)
