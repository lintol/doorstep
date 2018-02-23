"""Common routines for dask engine."""

import os
from dask import threaded

def execute(filename, module_name, metadata):
    """Import and run a workflow on a data file."""
    mod = __import__(module_name)
    return run(filename, mod, metadata)

def run(filename, mod, metadata):
    """Real runner for a given ltldoorstep processor module and datafile."""

    processor = mod.processor()
    workflow = processor.get_workflow(filename, metadata)

    threaded.get(workflow, 'output')

    return processor.compile_report(filename, metadata)
