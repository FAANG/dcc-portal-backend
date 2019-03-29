"""
Different constants that could be used by faang backend workflow
"""

# Addresses of elasticsearch servers
STAGING_NODE1 = 'wp-np3-e2:9200'
STAGING_NODE2 = 'wp-np3-e3:9200'
FALLBACK_NODE1 = 'wp-p2m-e2:9200'
FALLBACK_NODE2 = 'wp-p2m-e3:9200'
PRODUCTION_NODE1 = 'wp-p1m-e2:9200'
PRODUCTION_NODE2 = 'wp-p1m-e3:9200'

# Paths for rsync command (is used during syncing process between staging and production elasticsearch servers)
FROM = '/nfs/public/rw/reseq-info/elastic_search_staging/snapshot_repo/es6_faang_repo/'
TO = '/nfs/public/rw/reseq-info/elastic_search/snapshot_repo/es6_faang_repo/'

# Universities abbreviations to use in create_protocols script
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