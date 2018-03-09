from . import dask_threaded
from . import dask_distributed
from . import pachyderm
from . import docker

engines = {
    'dask.threaded': dask_threaded.DaskThreadedEngine,
    'dask.distributed': dask_distributed.DaskDistributedEngine,
    'docker': docker.DockerEngine,
    'pachyderm': pachyderm.PachydermEngine,
}
