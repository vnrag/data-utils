import os
import json

conf_folder = os.path.dirname(os.path.abspath(__file__))
conf_file_loc = os.path.join(conf_folder, 'config.json')

def load_config():
    with open(conf_file_loc) as config_file:
        data = json.load(config_file)
    return data