from . import dask_threaded
from . import dask_distributed
from . import pachyderm

engines = {
    'dask.threaded': dask_threaded.DaskThreadedEngine,
    'dask.distributed': dask_distributed.DaskDistributedEngine,
    'pachyderm': pachyderm.PachydermEngine,
}
