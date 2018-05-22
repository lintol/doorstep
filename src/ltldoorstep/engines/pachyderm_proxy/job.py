import asyncio
import pypachy
from .job_error import JobFailedException, JobKilledException

class PachydermJob:
    _id = None
    _state = None

    @staticmethod
    async def _execute(*args):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, *args)

    def __init__(self, clients, jid):
        self._id = jid
        self._clients = clients

    def pull_logs(self):
        return self._clients['pps'].get_logs(job_id=self._id)

    def get_id(self):
        return self._id

    @classmethod
    async def list_jobs(cls, clients, pipeline):
        jobs = []
        loop = asyncio.get_event_loop()
        raw_jobs = await cls._execute(clients['pps'].list_job, pipeline.get_definition())

        for raw_job in raw_jobs.job_info:
            jobs.append(cls.from_raw(clients, raw_job))

        return jobs

    @classmethod
    def from_raw(cls, clients, raw_job):
        job = cls(clients, jid=raw_job.job.id)
        job.update_from_raw(raw_job)
        return job

    def get_state(self):
        return self._state

    def update_from_raw(self, raw_job):
        self._state = raw_job.state

    async def update(self):
        loop = asyncio.get_event_loop()
        raw_job = await self._execute(self._clients['pps'].inspect_job, self._id)
        self.update_from_raw(raw_job)

    def check(self):
        if self._state == pypachy.JOB_FAILURE:
            logs = self.pull_logs()
            raise JobFailedException(job=self, logs=logs)
        elif self._state == pypachy.JOB_KILLED:
            raise JobKilledException(job=self)
