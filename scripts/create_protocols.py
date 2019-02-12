from elasticsearch import Elasticsearch

UNIVERSITIES = {
    "ROSLIN": "Roslin Institute (Edinburgh, UK)",
    "INRA": "French National Institute for Agricultural Research",
    "WUR": "Wageningen University and Research",
    "UCD": "University of California, Davis (USA)",
    "USU": "Utah State University (USA)",
    "DEDJTR": "Department of Economic Development, Jobs, Transport and Resources (Bundoora, Australia)",
    "FBN": "Leibniz Institute for Farm Animal Biology (Dummerstorf, Germany)",
    "TAMU": "Texas A&M University",
    "UIC": "University of Illinois at Chicago (USA)",
    "ESTEAM": "ESTeam Paris SUD (France)",
    "ISU": "Iowa State University",
    "KU": "Konkuk University (Seoul, Korea)",
    "NUID": "University College Dublin (Dublin, Ireland)",
    "NMBU": "Norwegian University of Life Sciences (Norway)",
    "UIUC": "University of Illinois at Urbana–Champaign (USA)",
    "UD": "University of Delaware (USA)",
    "UDL": "University of Lleida (Catalonia, Spain)",
    "ULE": "University of León (León, Spain)",
    "USDA": "The United States Department of Agriculture",
}


def create_sample_protocol():
    es = Elasticsearch(['wp-np3-e2', 'wp-np3-e3'])
    results = es.search(index="specimen", size=100000)
    entries = {}
    for result in results["hits"]["hits"]:
        if "specimenFromOrganism" in result["_source"] and 'specimenCollectionProtocol' in \
                result['_source']['specimenFromOrganism']:
            key = result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['filename']
            url = result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['url']
            try:
                protocol_type = result['_source']['specimenFromOrganism']['specimenCollectionProtocol']['url'].split("/")[5]
            except:
                protocol_type = ""
            parsed = key.split("_")
            if parsed[0] in UNIVERSITIES:
                name = UNIVERSITIES[parsed[0]]
                protocol_name = " ".join(parsed[2:-1])
                date = parsed[-1].split(".")[0]
                entries.setdefault(key, {"specimen": [], "universityName": "", "protocolDate": "",
                                         "protocolName": "", "key": "", "url": "", "protocolType": ""})
                specimen = dict()
                specimen["id"] = result["_id"]
                specimen["organismPartCellType"] = result["_source"]["cellType"]["text"]
                specimen["organism"] = result["_source"]["organism"]["organism"]["text"]
                specimen["breed"] = result["_source"]["organism"]["breed"]["text"]
                specimen["derivedFrom"] = result["_source"]["derivedFrom"]

                entries[key]["specimen"].append(specimen)
                entries[key]['universityName'] = name
                entries[key]['protocolDate'] = date[0:4]
                entries[key]["protocolName"] = protocol_name
                entries[key]["key"] = key
                if protocol_type in ["analysis", "assays", "samples"]:
                    entries[key]["protocolType"] = protocol_type
                entries[key]["url"] = url
    for item in list(entries):
        es.index(index='protocol_samples', doc_type="_doc", id=item, body=entries[item])


def create_experiment_protocol():
    pass


if __name__ == "__main__":
    create_sample_protocol()
    create_experiment_protocol()
