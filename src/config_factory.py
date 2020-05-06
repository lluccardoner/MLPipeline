import json


class ConfigFactory:

    def __init__(self):
        pass

    @staticmethod
    def create_config(path):
        with open(path) as json_file:
            config = json.load(json_file)

        return config
