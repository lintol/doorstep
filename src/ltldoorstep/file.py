import boto3
import contextlib
import os
import tempfile
import shutil


class DummyFileManager:
    def get(self, filename):
        return filename

class S3FileManager:
    def __init__(self, bucket_name, local_directory):
        rsrce = boto3.resource('s3')
        self._bucket = rsrce.Bucket(bucket_name)
        self._local = local_directory

    def get(self, filename):
        local_filename = os.path.join(self._local, os.path.basename(filename))
        self._bucket.download_file(filename, local_filename)

        return local_filename

    @classmethod
    @contextlib.contextmanager
    def make(cls, bucket):
        with tempfile.TemporaryDirectory() as local_directory:
            yield cls(bucket, local_directory)

@contextlib.contextmanager
def make_file_manager(bucket):
    if bucket:
        with S3FileManager.make(bucket) as file_manager:
            yield file_manager
    else:
        yield DummyFileManager()
