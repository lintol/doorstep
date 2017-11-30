"""Proxy class for a Pachyderm Repo."""

import asyncio
from contextlib import contextmanager
from .commit import PachydermCommit
import threading
import sys
import janus

SUB_REFRESH_RATE = 5. # Seconds between streaming calls from gRPC


class PachydermRepo:
    """Proxy class for a Pachyderm Repo."""

    _default_branch = 'master'

    def __init__(self, clients, name):
        self._name = name
        self._clients = clients
        self._master = PachydermCommit(self._clients, self._name, self._default_branch)
        self._commit_watch_queues = {}
        self._commit_watch_futures = {}

    def get_name(self):
        """Name of this repo."""

        return self._name

    def pull_file(self, path):
        """Pull a file from master."""

        return self._master.pull_file(path)

    async def stop_watching_commits(self, branch=None):
        if not branch:
            branch = self._master

        fut, cancel = self._commit_watch_futures[branch.get_name()]

        cancel.set()
        await fut

    async def watch_commits(self, branch=None, from_commit=None):
        """Fill queue of commits."""

        if not branch:
            branch = self._master

        if branch.get_name() in self._commit_watch_queues:
            queue = self._commit_watch_queues[branch.get_name()]
        else:
            client = self._clients['pfs']
            loop = asyncio.get_event_loop()
            queue = janus.Queue(loop=loop)

            cancel_event = threading.Event()
            commit_future = loop.run_in_executor(
                None,
                _watch_commits_real,
                loop,
                queue.sync_q,
                self._clients,
                self.get_name(),
                branch.get_name(),
                from_commit.get_name() if from_commit else None,
                cancel_event
            )

            self._commit_watch_futures[branch.get_name()] = (commit_future, cancel_event)
            self._commit_watch_queues[branch.get_name()] = queue

        return queue.async_q

    def subscribe_commit(self, branch=None, from_commit=None):
        """Subscribe to commits on a branch."""

        if not branch:
            branch = self._master

        client = self._clients['pfs']
        commit_gen = _subscribe_commit_real(
            client,
            self._name,
            branch.get_name(),
            from_commit.get_name() if from_commit else None
        )

        for commit in commit_gen:
            yield PachydermCommit.from_raw(commit, self._clients)

    @contextmanager
    def make_commit(self, commit):
        """Add a new self-contained commit to this repo."""

        self._clients['pfs'].start_commit(self._name, commit)
        try:
            yield PachydermCommit(self._clients, self._name, commit)
        finally:
            self._clients['pfs'].finish_commit('%s/%s' % (self._name, commit))

@contextmanager
def make_repo(clients, name):
    """Create a temporary repo."""

    clients['pfs'].create_repo(name)
    try:
        yield PachydermRepo(clients, name)
    finally:
        clients['pfs'].delete_repo(name)

def _subscribe_commit_real(clients, repo_name, branch_name, from_commit_name, cancel_event):
    """Subscribe to commits on a branch."""

    for commit in clients['pfs'].refreshing_subscribe_commit(repo_name, branch_name, from_commit_name, timeout=SUB_REFRESH_RATE, cancel=cancel_event):
        yield commit

def _watch_commits_real(loop, queue, clients, repo_name, branch_name, from_commit_name, cancel_event):
    """Observe commits comming from API."""

    for commit in _subscribe_commit_real(clients, repo_name, branch_name, from_commit_name, cancel_event):
        queue.put(PachydermCommit.from_raw(commit, clients))
