from . import dask
from . import pachyderm

engines = {
    'dask.threaded': dask.DaskThreadedProcessor,
    'dask.distributed': dask.DaskDistributedProcessor,
    'pachyderm': pachyderm.PachydermProcessor,
}
