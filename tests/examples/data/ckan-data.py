import json
import requests
import pprint

def create_json_from_ckanopenindustry():
    # list of tags
    request_tags = requests.get("https://ckan.ev.openindustry.in/api/action/tag_list")
    tag_list = request_tags.json()
    # list of dataset titles
    datasets = requests.get("https://ckan.ev.openindustry.in/api/action/package_list")
    dataset_list = datasets.json()
    # get the dataset ids from the json
    dataset_id = dataset_list["result"]
    sample_data_1 = requests.get("https://ckan.ev.openindustry.in/api/action/package_show?id=dispensing-by-contractor")
    sample_1_json = sample_data_1.json()
    pprint.pprint(sample_1_json)
    return sample_1_json
    # sample_data_2 = requests.get("https://ckan.ev.openindustry.in/api/action/package_show?id=scheduled-historic-monument-areas")
    # sample_2_json = sample_data_2.json()
    # pprint.pprint(sample_2_json)
    # sample_data_3 = requests.get("https://ckan.ev.openindustry.in/api/action/package_show?id=recycling-centres")
    # sample_3_json = sample_data_3.json()
    # pprint.pprint(sample_3_json)
    
sample_data_2 = requests.get("https://ckan.ev.openindustry.in/api/action/package_show?id=scheduled-historic-monument-areas")
sample_2_json = sample_data_2.json()
pprint.pprint(sample_2_json)


if __name__ == '__main__':
    create_json_from_ckanopenindustry()
