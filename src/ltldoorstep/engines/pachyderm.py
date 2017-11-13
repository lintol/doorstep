"""Engine for running a job on a Pachyderm cluster."""

from contextlib import contextmanager
import os
import time
import sys
import json
import uuid
import pypachy
from .pachyderm_proxy.repo import make_repo
from .pachyderm_proxy.pipeline import make_pipeline


class PachydermEngine:
    """Allow execution of workflows on a Pachyderm cluster."""

    _retry_count = 120
    _retry_delay = 1.0
    _retry_processing_count = 50
    pipeline_definition = None

    def __init__(self):
        self.set_clients(
            pps=pypachy.pps_client.PpsClient(),
            pfs=pypachy.pfs_client.PfsClient()
        )

        self._pipeline_template = os.path.join(
            os.path.dirname(__file__),
            'pachyderm_proxy',
            'doorstep.json'
        )

    def get_definition(self):
        """Load and set the pipeline definition."""

        if not self.pipeline_definition:
            with open(self._pipeline_template, 'r') as file_obj:
                self.pipeline_definition = json.load(file_obj)

        return self.pipeline_definition

    def get_clients(self):
        """Get the client objects used to communicate with Pachyderm."""

        return (self._clients['pps'], self._clients['pfs'])

    def set_clients(self, pps, pfs):
        """Set the client objects to communicate with Pachyderm."""

        self._clients = {
            'pps': pps,
            'pfs': pfs
        }

    def add_processor(self, module_name, content, session):
        """Mark a module_name as a processor."""

        filename = '/%s.py' % module_name
        self._add_file('processors', filename, content, session)

    def add_data(self, filename, content, session, bucket=None):
        """Prepare to send a data file to Pachyderm."""

        filename = '/%s' % filename
        self._add_file('data', filename, content, session, bucket)

    def _add_file(self, category, filename, content, session, bucket=None):
        """Transfer file to Pachyderm."""

        with session[category].make_commit('master') as commit:
            if bucket:
                commit.put_file_url(
                    filename,
                    's3://%s/%s' % (bucket, content)
                )
            else:
                commit.put_file_bytes(
                    filename,
                    content
                )

    @contextmanager
    def make_session(self):
        """Set up a workflow session.

        This creates a self-contained set of Pachyderm constructs representing our operation.
        """

        clients = self._clients

        name = 'doorstep-%s' % str(uuid.uuid4())
        data_name = '%s-data' % name
        processors_name = '%s-processors' % name

        pipeline_definition = self.get_definition()

        with make_repo(clients, data_name) as data_repo, \
                make_repo(clients, processors_name) as processors_repo:
            session = {
                'name': name,
                'data': data_repo,
                'processors': processors_repo
            }
            with make_pipeline(clients, pipeline_definition, session) as pipeline:
                session['pipeline'] = pipeline
                yield session

    def run(self, filename, workflow_module, bucket=None):
        """Execute the pipeline on a Pachyderm cluster."""

        with self.make_session() as session:
            with open(workflow_module, 'r') as file_obj:
                self.add_processor('processor', file_obj.read().encode('utf-8'), session)

            if bucket:
                content = filename
            else:
                with open(filename, 'r') as file_obj:
                    content = file_obj.read().encode('utf-8')

            # TODO: safely set file extension
            self.add_data('data.csv', content, session, bucket)

            results = self.monitor_pipeline(session)

        return results

    @staticmethod
    def _wait_for_pipeline(pipeline, retry_count, retry_processing_count, retry_delay):
        """Wait until pipeline has completed."""

        def _tick_callback():
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(retry_delay)

        # Wait for pipeline run to start
        pipeline.wait_for_run(
            retry_count,
            tick_callback=_tick_callback,
            error_suffix=" to start"
        )

        # Wait for pipeline run to finish
        return pipeline.wait_for_run(
            retry_processing_count,
            (pypachy.JOB_STARTING, pypachy.JOB_RUNNING),
            tick_callback=_tick_callback,
            error_suffix=" to finish"
        )

    def monitor_pipeline(self, session):
        """Check pipeline for completion and process results."""

        job = self._wait_for_pipeline(
            session['pipeline'],
            self._retry_count,
            self._retry_processing_count,
            self._retry_delay
        )

        if job.state == pypachy.JOB_SUCCESS:
            print('Success')
        elif job.state == pypachy.JOB_FAILURE:
            logs = self._clients['pps'].get_logs(job_id=job.job.id)
            print(logs[0])
            print('\n'.join([log.message.rstrip() for log in logs]))
            raise RuntimeError('Job failed')
        elif job.state == pypachy.JOB_KILLED:
            raise RuntimeError('Job was killed')
        else:
            raise RuntimeError('Unknown job status')

        output = session['pipeline'].pull_output('/doorstep.out')

        return ['\n'.join([line.decode('utf-8') for line in output])]
