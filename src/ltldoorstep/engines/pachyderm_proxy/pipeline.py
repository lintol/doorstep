"""Proxy class for a Pachyderm Pipeline."""

import time
from contextlib import contextmanager
from .repo import PachydermRepo
from .pipeline_error import PipelineException
from .job import PachydermJob

class PachydermPipeline:
    """Proxy class for a Pachyderm Pipeline."""

    _current_job = None

    def __init__(self, clients, definition):
        self._pipeline = definition['pipeline']
        self._clients = clients
        self._definition = definition
        self._output_repo = PachydermRepo(clients, self.get_name())

    def get_definition(self):
        """Definition of this pipeline."""

        return self._pipeline

    def get_name(self):
        """Name of this pipeline."""

        return self._pipeline['name']

    async def list_jobs(self):
        """Find jobs related to this pipeline."""

        return await PachydermJob.list_jobs(self._clients, self)

    def pull_output(self, path):
        """Pull a file from the output repo."""

        return self._output_repo.pull_file(path)

    async def check_started(self):
        """Check whether started.

        Indicate whether we have started a run, and return job if so.
        """

        job_info = await self.list_jobs()

        if job_info:
            # TODO: There should only be one job, but check
            return job_info[0]

        return False

    async def watch_commits(self):
        """Fill queue of commits."""

        return await self._output_repo.watch_commits()

    async def stop_watching_commits(self):
        """Stop watching commits."""

        return await self._output_repo.stop_watching_commits()

    def subscribe_output_commit(self):
        """Return commits from the output repo."""

        yield from self._output_repo.subscribe_commit()

    async def wait_for_run(self, retry_count, ignore_states=(), tick_callback=None, error_suffix=""):
        """Wait until the this pipeline runs to a certain point."""

        job = self._current_job

        for __ in range(retry_count):
            if job and not job.get_state() in ignore_states:
                break

            if tick_callback:
                await tick_callback()

            if job:
                await job.update()
            else:
                job = await self.check_started()

            if job:
                job.check()
        else:
            raise PipelineException(_("Gave up waiting for pipeline job%s") % error_suffix)

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
        # clients['pps'].delete_pipeline(pipeline.get_name(), delete_jobs=True, delete_repo=True)
        print('deleting')
