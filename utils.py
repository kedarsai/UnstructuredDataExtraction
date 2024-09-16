# utils.py

import yaml

def load_yaml_config(yaml_file_path):
    with open(yaml_file_path, 'r') as file:
        fields_config = yaml.safe_load(file)['fields']
    return fields_config
