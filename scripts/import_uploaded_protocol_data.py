import os
import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, RequestsHttpConnection
from constants import *

ES_USER = os.getenv('ES_USER')
ES_PASSWORD = os.getenv('ES_PASSWORD')
es = Elasticsearch([PRODUCTION__NODE_ELASTIC_CLOUD], connection_class=RequestsHttpConnection, http_auth=(ES_USER, ES_PASSWORD), use_ssl=True)

protocols_url = {
    'protocol_samples': [
        'https://api.faang.org/files/protocols/samples/'
    ],
    'protocol_files': [
        'https://api.faang.org/files/protocols/experiments/', 
        'https://api.faang.org/files/protocols/assays/'
    ],
    'protocol_analysis': [
        'https://api.faang.org/files/protocols/analyses/',
        'https://api.faang.org/files/protocols/analysis/'
    ]
}

for index in protocols_url:
    count = 0
    urls = protocols_url[index]
    for url in urls:
        r = requests.get(url)
        html_text = r.text
        soup = BeautifulSoup(html_text, 'html.parser')
        for link in soup.find_all('a'):
            protocol_file = link.get('href')
            if protocol_file != '../':
                key = requests.utils.unquote(protocol_file)
                parsed = protocol_file.strip().split("_")
                # Parsing protocol name
                if 'SOP' in parsed:
                    protocol_name = requests.utils.unquote(" ".join(parsed[2:-1]))
                else:
                    protocol_name = requests.utils.unquote(" ".join(parsed[1:-1]))
                if index == 'protocol_samples' or index == 'protocol_analysis':
                    if not es.exists(index, id=key):
                        count += 1
                        # Parsing university name
                        if parsed[0] == 'WUR':
                            university_name = 'WUR'
                        elif parsed[0] not in UNIVERSITIES:
                            university_name = None
                        else:
                            university_name = UNIVERSITIES[parsed[0]]
                        # Parsing date
                        date = parsed[-1].split(".pdf")[0][:4]
                        protocol_data = {
                            "universityName": university_name,
                            "protocolDate": date,
                            "protocolName": protocol_name, 
                            "key": key,
                            "url": f"{url}{protocol_file}"
                        }
                        if index == 'protocol_samples':
                            protocol_data["specimens"] = []
                        elif index == 'protocol_analysis':
                            protocol_data["analyses"] = []
                        es.create(index, id=key, body=protocol_data)
                else:
                    r = requests.get(f"https://api.faang.org/data/protocol_files/_search/?search={protocol_file}").json()
                    if (r['hits']['total']['value'] == 0):
                        count += 1
                        protocol_data = {
                            "experiments": [],
                            "experimentTarget": "",
                            "assayType": "",
                            "name": protocol_name,
                            "filename": key,
                            "key": key,
                            "url": f"{url}{protocol_file}"
                        }
                        es.create(f"{index}", id=key, body=protocol_data)

    print(f"Added {count} protocols to index {index}")