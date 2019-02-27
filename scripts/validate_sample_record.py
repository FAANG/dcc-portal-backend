import requests


def get_ruleset_version():
    url = 'https://api.github.com/repos/FAANG/faang-metadata/releases'
    response = requests.get(url).json()
    return response[0]['tag_name']