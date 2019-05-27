import uuid
import json
from autobahn.wamp.exception import ApplicationError
import logging
import requests
from ltldoorstep.metadata import DoorstepContext
from ltldoorstep.ini import DoorstepIni
from ltldoorstep.file import make_file_manager
from retry import api as retry_api
import os

ALLOWED_FORMATS = ('CSV', 'GeoJSON')

def ckan_retry(f, **kwargs):
    return retry_api.retry_call(f, fkwargs=kwargs, tries=6, delay=1)

async def do_crawl(component, url, workflow, printer, publish):
    from ckanapi import RemoteCKAN
    client = RemoteCKAN(url, user_agent='lintol-doorstep-crawl/1.0 (+http://lintol.io)')

    packages = ckan_retry(client.action.package_list)

    for package in packages:
        package_metadata = ckan_retry(client.action.package_show, id=package)

        ini = DoorstepIni(context_package=package_metadata) # classes = studley case
        for resource in ini.package['resources']:
            if resource['format'] in ALLOWED_FORMATS:
                if workflow:
                    logging.error(resource['url'])
                    r = requests.get(resource['url'])
                    with make_file_manager(content={'data.csv': r.text}) as file_manager:
                        filename = file_manager.get('data.csv')
                        result = await execute_workflow(component, filename, workflow, ini)
                        print(result)
                        if result:
                            printer.build_report(result)
                if publish:
                    result = await announce_resource(component, resource, ini, url)
            else:
                if not resource['format']:
                    print(resource)
                logging.warn("Not allowed format: {}".format(resource['format']))
    printer.print_output()

async def announce_resource(component, resource, ini, source):
    """When we join the server, execute the client workflow."""

    component.publish('com.ltldoorstep.event_found_resource', resource['id'], resource, ini.to_dict(), source)


async def execute_workflow(component, filename, workflow, ini):
    """When we join the server, execute the client workflow."""

    #with open(self._filename, 'r') as file_obj:
    #    content = file_obj.read()
    basefilename = os.path.basename(filename)

    with open(workflow, 'r') as file_obj:
        module = file_obj.read()
    workflow = os.path.basename(workflow)

    definitions = {
        str(uuid.uuid4()): DoorstepContext.from_dict({
            'module': workflow
        })
    }
    if not ini:
        ini = DoorstepIni(definitions=definitions)

    if type(ini) is str:
        with open(ini, 'r') as file_obj:
            ini = DoorstepIni.from_dict(json.load(file_obj))
    elif not ini.definitions:
        ini.definitions = definitions

    component._server, component._session = await component.call('com.ltldoorstep.engage')

    print('C', ini.to_dict()['definitions'])
    await component.call_server('processor.post', {workflow: module}, ini.to_dict())
    content = "file://{}".format(os.path.abspath(filename))

    logging.error("Sending: {}".format(content))
    await component.call_server('data.post', basefilename, content, True)

    try:
        result = await component.call_server('report.get')
    except ApplicationError as e:
        logging.error(e)
        result = None

    result = json.loads(result)
    logging.error(result)
    return result

    #temp commented out
    # if component._printer and result:
        # component._printer.build_report(result)
