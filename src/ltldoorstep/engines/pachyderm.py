from contextlib import contextmanager
import logging
from functools import reduce
from dask import threaded
from dask.distributed import Client
import os
import sys
import json
import uuid
import pypachy
import time

class PachydermCommit:
    def __init__(self, clients, repo, name):
        self._repo = repo
        self._name = name
        self._clients = clients

    def put_file_bytes(self, filename, content):
        self._clients['pfs'].put_file_bytes(
            self.get_full_name(),
            filename,
            content
        )

    def put_file_url(self, filename, url):
        self._clients['pfs'].put_file_url(
            self.get_full_name(),
            filename,
            url
        )

    def get_name(self):
        return self._name

    def get_full_name(self):
        return '%s/%s' % (self._repo, self._name)

    def get_file(self, path):
        return self._clients['pfs'].get_file(self.get_full_name(), path)


class PachydermPipeline:
    def __init__(self, clients, definition):
        self._pipeline = definition['pipeline']
        print(self._pipeline)
        self._clients = clients
        self._definition = definition
        self._output_repo = PachydermRepo(clients, self.get_name())

    def get_name(self):
        return self._pipeline['name']

    def get_jobs(self):
        return self._clients['pps'].list_job(self._pipeline).job_info

    def get_output(self, path):
        return self._output_repo.get_file(path)


class PachydermRepo:
    def __init__(self, clients, name):
        self._name = name
        self._clients = clients
        self._master = PachydermCommit(self._clients, self._name, 'master')

    def get_name(self):
        return self._name

    def get_file(self, path):
        return self._master.get_file(path)

    @contextmanager
    def make_commit(self, commit):
        self._clients['pfs'].start_commit(self._name, commit)
        try:
            yield PachydermCommit(self._clients, self._name, commit)
        finally:
            self._clients['pfs'].finish_commit('%s/%s' % (self._name, commit))

@contextmanager
def make_repo(clients, name):
    clients['pfs'].create_repo(name)
    try:
        yield PachydermRepo(clients, name)
    finally:
        clients['pfs'].delete_repo(name)

@contextmanager
def make_pipeline(clients, definition, session):
    definition['pipeline']['name'] = session['name']
    definition['input']['cross'][0]['atom']['repo'] = session['processors'].get_name()
    definition['input']['cross'][1]['atom']['repo'] = session['data'].get_name()

    clients['pps'].create_pipeline(**definition)
    pipeline = PachydermPipeline(clients, definition)
    try:
        yield pipeline
    finally:
        clients['pps'].delete_pipeline(pipeline.get_name(), delete_jobs=True, delete_repo=True)

class PachydermProcessor:
    _retry_count = 120
    _retry_delay = 1.0
    _retry_processing_count = 50

    def __init__(self):
        self._clients = {
            'pps': pypachy.pps_client.PpsClient(),
            'pfs': pypachy.pfs_client.PfsClient()
        }

    def add_processor(self, module_name, content, session):
        filename = '/%s.py' % module_name
        self._add_file('processors', filename, content, session)

    def add_data(self, filename, content, session, bucket=None):
        filename = '/%s' % filename
        self._add_file('data', filename, content, session, bucket)

    def _add_file(self, category, filename, content, session, bucket=None):
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
        clients = self._clients

        name = 'doorstep-%s' % str(uuid.uuid4())
        data_name = '%s-data' % name
        processors_name = '%s-processors' % name

        pipeline_template = os.path.join(
            os.path.dirname(__file__),
            'pachyderm',
            'doorstep.json'
        )

        with open(pipeline_template, 'r') as f:
            pipeline_definition = json.load(f)

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
        with self.make_session() as session:
            with open(workflow_module, 'r') as f:
                self.add_processor('processor', f.read().encode('utf-8'), session)

            if bucket:
                content = filename
            else:
                with open(filename, 'r') as f:
                    content = f.read().encode('utf-8')

            self.add_data('data.csv', content, session, bucket)

            results = self.execute_pipeline(session)

        return results

    def execute_pipeline(self, session):
        pipeline = session['pipeline']

        for attempt in range(self._retry_count):
            job_info = pipeline.get_jobs()
            if job_info:
                break
            time.sleep(self._retry_delay)
            sys.stdout.write('.')
            sys.stdout.flush()
        else:
            raise RuntimeError('Gave up waiting for pipeline job to start')

        # There should only be one job
        job = job_info[0]

        for attempt in range(self._retry_processing_count):
            if job.state not in (pypachy.JOB_STARTING, pypachy.JOB_RUNNING):
                break
            time.sleep(self._retry_delay)
            job = pipeline.get_jobs()[0]
            sys.stdout.write('.')
            sys.stdout.flush()
        else:
            raise RuntimeError('Gave up waiting for job to finish')

        print()

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

        output = pipeline.get_output('/doorstep.out')

        return ['\n'.join([line.decode('utf-8') for line in output])]
