# Copyright 2017 Yuval Kalugny
# (with extension by Phil Weir)
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from pypachy.pfs_client import PfsClient
from pypachy.pfs_pb2 import *
from pypachy.pfs_pb2_grpc import *
import grpc

class PfsClientWrapper(PfsClient):
    """This is a workaround for the lack of cancellation in gRPC's server-stream Python functionality."""
    def refreshing_subscribe_commit(self, repo_name, branch, from_commit_id=None, timeout=None, cancel=None):
        repo = Repo(name=repo_name)
        req = SubscribeCommitRequest(repo=repo, branch=branch)

        commit = None

        # Check whether thread-safe event is set
        while not cancel.is_set():
            # Set the most recent desired commit
            if from_commit_id is not None:
                getattr(req, 'from').CopyFrom(Commit(repo=repo, id=from_commit_id))

            try:
                # Pull back relevant commits
                commit = self.stub.SubscribeCommit(req, timeout=timeout).next()

                # If it's not one we have, yield it
                if from_commit_id != commit.commit.id:
                    yield commit

                # Update to record the fact we have this commit
                from_commit_id = commit.commit.id

            except grpc._channel._Rendezvous as e:
                pass
