"""Common routines for dask engine."""

from dask import threaded

def execute(filename, module_name):
    """Import and run a workflow on a data file."""
    mod = __import__(module_name)
    return run(filename, mod)

def run(filename, mod):
    """Real runner for a given ltldoorstep processor module and datafile."""

    processor = mod.processor()
    workflow = processor.get_workflow(filename)

    return threaded.get(workflow, 'output')
