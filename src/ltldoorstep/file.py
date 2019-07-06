import boto3
import contextlib
import os
import tempfile
import shutil


class DummyFileManager:
    def get(self, filename):
        return filename

class TempFileManager:
    def __init__(self, content, local_directory):
        self._local = local_directory

        for filename, body in content.items():
            local_filename = os.path.join(self._local, os.path.basename(filename))
            mode = 'wb' if type(body) == bytes else 'w'
            with open(local_filename, mode) as file_obj:
                file_obj.write(body)

    def get(self, filename):
        local_filename = os.path.join(self._local, os.path.basename(filename))
        return local_filename

    @classmethod
    @contextlib.contextmanager
    def make(cls, content):
        with tempfile.TemporaryDirectory() as local_directory:
            yield cls(content, local_directory)

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
def make_file_manager(bucket=None, content=None):
    if bucket:
        with S3FileManager.make(bucket) as file_manager:
            yield file_manager
    elif content:
        with TempFileManager.make(content) as file_manager:
            yield file_manager
    else:
        yield DummyFileManager()
