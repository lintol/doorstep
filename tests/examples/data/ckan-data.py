import json
import requests
import pprint

def create_json():
    # make the http request
    tags = requests.get("https://ckan.ev.openindustry.in/api/action/tag_list")
    # load ckan response into json file
    tag_list = tags.json()
    # pprint.pprint(tag_list)

    # get package list
    # datasets = requests.get("https://ckan.ev.openindustry.in/api/action/package_list")
    # dataset_list = datasets.json()
    # get the dataset ids from the json
    # dataset_id = dataset_list["result"]
    # pprint.pprint(dataset_id)
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
