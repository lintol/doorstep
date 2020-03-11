from . import dask_threaded
from . import pachyderm
from . import docker
from . import openfaas

engines = {
    'dask.threaded': dask_threaded.DaskThreadedEngine,
    # Not currently supported (segfaults): 'dask.distributed': dask_distributed.DaskDistributedEngine,
    'docker': docker.DockerEngine,
    'openfaas': openfaas.OpenFaaSEngine,
    'pachyderm': pachyderm.PachydermEngine,
}
