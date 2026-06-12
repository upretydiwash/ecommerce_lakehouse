import json

class ConfigLoader:
    @staticmethod
    def load_config(config_file_path):
        with open(config_file_path, 'r') as config_file:
            config = json.load(config_file)
        return config