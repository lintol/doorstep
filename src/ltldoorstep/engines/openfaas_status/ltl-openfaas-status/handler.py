from requests.auth import HTTPBasicAuth
import json
import requests

def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """

    with open('/var/openfaas/secrets/authentication-secret', 'r') as f:
        openfaas_cred = f.read().strip()

    rq = requests.get(
        f'http://gateway.srv-openfaas.svc.cluster.local:8080/system/functions',
        json={
        },
        auth=HTTPBasicAuth('admin', openfaas_cred)
    )

    content = json.loads(rq.content)

    fns = [
        {
            'name': fn['name'],
            'available': fn['availableReplicas'],
            'total': fn['replicas'],
        }
        for fn in content
    ]

    return json.dumps(fns)
