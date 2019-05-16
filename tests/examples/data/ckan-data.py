import json
import requests
import pprint

def get_metadata():
    # list of tags
    # request_tags = requests.get("https://ckan.ev.openindustry.in/api/3/action/tag_list")
    # tag_list = request_tags.json()
    # list of dataset titles
    # datasets = requests.get("https://ckan.ev.openindustry.in/api/3/action/package_list")
    # dataset_list = datasets.json()
    # get the dataset ids from the json
    # dataset_id = dataset_list["result"]
    sample_data_1 = requests.get("https://ckan.ev.openindustry.in/api/3/action/package_show?id=dispensing-by-contractor")
    sample_1_json = sample_data_1.json()
    # download_dataset(sample_1_json)
    # pprint.pprint(sample_1_json)
    # return sample_1_json
    # sample_data_2 = requests.get("https://ckan.ev.openindustry.in/api/action/package_show?id=scheduled-historic-monument-areas")
    # sample_2_json = sample_data_2.json()
    # pprint.pprint(sample_2_json)
    # sample_data_3 = requests.get("https://ckan.ev.openindustry.in/api/action/package_show?id=recycling-centres")
    # sample_3_json = sample_data_3.json()
    # pprint.pprint(sample_3_json)
    
# sample_data_2 = requests.get("https://ckan.ev.openindustry.in/api/action/package_show?id=scheduled-historic-monument-areas")
# sample_2_json = sample_data_2.json()
# pprint.pprint(sample_2_json)

def login(username, password):
    '''
    Login to CKAN.

    Returns a ``requests.Session`` instance with the CKAN
    session cookie.
    '''
    s = requests.Session()
    data = {'login': username, 'password': password}
    url = "https://ckan.ev.openindustry.in/login_generic"
    r = s.post(url, data=data)
    if 'field-login' in r.text:
        # Response still contains login form
        raise RuntimeError('Login failed.')
    return s

def download_dataset(session, sample_1_json):
    # url = '{ckan}/dataset/{pkg}/resource/{res}/download/'.format(ckan=sample_1_json, pkg=dispensing-by-contractor, res=res_id)
    url = "https://ckan.ev.openindustry.in/api/3/action/package_show?id=dispensing-by-contractor/download"
    return session.get(url).content

if __name__ == '__main__':
    get_metadata()
