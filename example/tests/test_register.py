import os
from dask.threaded import get

from processors import registers

def testing_register_pass():
    path = os.path.join('data', 'records.json')
    workflow = registers.return_workflow(path)
    results = get(workflow, 'output_check')

    assert len(results)==1
    report = results[0]
    assert report['reg_check_valid'][2] == ['The Portuguese Republic']
   

       	
	
