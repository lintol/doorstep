"""Proxy class for a Pachyderm Repo."""

from contextlib import contextmanager
from .commit import PachydermCommit

class PachydermRepo:
    """Proxy class for a Pachyderm Repo."""

    _default_branch = 'master'

    def __init__(self, clients, name):
        self._name = name
        self._clients = clients
        self._master = PachydermCommit(self._clients, self._name, self._default_branch)

    def get_name(self):
        """Name of this repo."""

        return self._name

    def pull_file(self, path):
        """Pull a file from master."""

        return self._master.pull_file(path)

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
