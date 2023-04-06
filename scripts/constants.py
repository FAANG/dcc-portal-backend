"""
Different constants that could be used by faang backend workflow
It is organized into sections General setting, and Script-specific sections
"""
from typing import Dict

######################################
# General setting
######################################
# Addresses of elasticsearch servers
STAGING_NODE1 = 'wp-np3-e2:9200'
STAGING_NODE2 = 'wp-np3-e3:9200'
FALLBACK_NODE1 = 'wp-p2m-e2:9200'
FALLBACK_NODE2 = 'wp-p2m-e3:9200'
PRODUCTION_NODE1 = 'wp-p1m-e2:9200'
PRODUCTION_NODE2 = 'wp-p1m-e3:9200'
PRODUCTION__NODE_ELASTIC_CLOUD = 'https://prj-ext-dev-faang-gcp-dr.es.europe-west2.gcp.elastic-cloud.com'
DEFAULT_PREFIX = 'faang_build_3'

ALIASES_IN_USE = {
    'faang_build_3_file': 'file',
    'faang_build_3_organism': 'organism',
    'faang_build_3_specimen': 'specimen',
    'faang_build_3_dataset': 'dataset',
    'faang_build_3_experiment': 'experiment',
    'protocol_files3': 'protocol_files',
    'protocol_samples3': 'protocol_samples',
    'summary_specimen': 'summary_specimen',
    'summary_organism': 'summary_organism',
    'summary_file': 'summary_file',
    'summary_dataset': 'summary_dataset',
    'faang_build_3_analysis': 'analysis',
    'faang_build_3_article': 'article'
}

# Current indices in use
TYPES = ['organism', 'specimen', 'file', 'experiment', 'dataset', 'analysis', 'article', 'log']

# Paths for rsync command (is used during syncing process between staging and production elasticsearch servers)
FROM = '/nfs/public/rw/reseq-info/elastic_search_staging/snapshot_repo/es6_faang_repo/'
TO = '/nfs/public/rw/reseq-info/elastic_search/snapshot_repo/es6_faang_repo/'

# species considered within FAANG project
SPECIES_DICT = {
  "9031": "Gallus gallus",
  "9913": "Bos taurus",
  "9823": "Sus scrofa",
  "9940": "Ovis aries",
  "9796": "Equus caballus",
  "9925": "Capra hircus"
}

#######################################
# Scripts specific, in alphabetic order
#######################################

# create_protocol
# Universities abbreviations to use in create_protocols script
UNIVERSITIES = {
    "ABDN": "University of Aberdeen (Aberdeen, UK)",
    "AGR": "AgResearch (New Zealand)",
    "AGS": "Agroscope (Switzerland)",
    "DEDJTR": "Department of Economic Development, Jobs, Transport and "
              "Resources (Bundoora, Australia)",
    "DIAGENODE": "Diagenode (Liège, Belgium)",
    "EHU": "University of the Basque Country (Spain)",
    "ESTEAM": "ESTeam Paris SUD (France)",
    "FBN": "Leibniz Institute for Farm Animal Biology (Dummerstorf, Germany)",
    "HCMR": "Hellenic Centre for Marine Research (Greece)",
    "INRA": "French National Institute for Agricultural Research (France)",
    "INRAE": "National Research Institute for Agriculture, Food and "
             "Environment (France)",
    "INSERM": "French National Institute of Health and Medical Research (France)",
    "INSERM-INRAE": "INSERM-INRAE",
    "IRTA": "Institute of Agrifood Research and Technology (Spain)",
    "ISU": "Iowa State University (USA)",
    "KU": "Konkuk University (Seoul, Korea)",
    "NUID": "University College Dublin (Dublin, Ireland)",
    "NMBU": "Norwegian University of Life Sciences (Norway)",
    "QAAFI-UQ": "University of Queensland (Australia)",
    "ROSLIN": "Roslin Institute (Edinburgh, UK)",
    "TAMU": "Texas A&M University (USA)",
    "UAL": "University of Alberta (Canada)",
    "UCD": "University of California, Davis (USA)",
    "UD": "University of Delaware (USA)",
    "UDL": "University of Lleida (Catalonia, Spain)",
    "UEDIN": "University of Edinburgh (Edinburgh, UK)",
    "UIC": "University of Illinois at Chicago (USA)",
    "UIDAHO": "University of Idaho (USA)",
    "UIUC": "University of Illinois at Urbana-Champaign (USA)",
    "ULE": "University of León (León, Spain)",
    "UNIPD": "University of Padua (Italy)",
    "UNL": "University of Nebraska-Lincoln (USA)",
    "UOB": "University of Birmingham (UK)",
    "USC": "University of Santiago de Compostela (Spain)",
    "USDA": "The United States Department of Agriculture (USA)",
    "USMARC": "United States Meat Animal Research Center (USA)",
    "USU": "Utah State University (USA)",
    "WSU": "Washington State University(USA)",
    "WU": "Wageningen University (Netherlands)",
    "ZIGR": "Polish Academy of Sciences Institute of Ichthyobiology and Aquaculture in Golysz (Poland)"
}

# create_summary
# Sex names
MALES = ['male', 'male genotypic sex', 'intact male', 'M', 'Male']
FEMALES = ['female', 'female genotypic sex', 'intact female', 'F', 'Female']

# import from biosample, ena, ena legacy

#standards
STANDARD_FAANG = 'FAANG'
STANDARD_LEGACY = 'Legacy'
STANDARD_BASIC = 'Legacy (basic)'

# keys are assay types and values are the corresponding technology
# expected to be used in import from ena and ena legacy
TECHNOLOGIES: Dict[str, str] = {
    'ATAC-seq': 'ATAC-seq',
    'methylation profiling by high throughput sequencing': 'BS-seq',
    'ChIP-seq': 'ChIP-seq',
    'DNase-Hypersensitivity seq': 'DNase-seq',
    'Hi-C': 'Hi-C',
    'microRNA profiling by high throughput sequencing': 'RNA-seq',
    'RNA-seq of coding RNA': 'RNA-seq',
    'RNA-seq of non coding RNA': 'RNA-seq',
    'RNA-seq of total RNA': 'RNA-seq',
    'transcription profiling by high throughput sequencing': 'RNA-seq',
    'whole genome sequencing assay': 'WGS',
    'CAGE-seq': 'CAGE-seq'
}

STANDARDS = {
    'FAANG Samples': STANDARD_FAANG,
    'FAANG Legacy Samples': STANDARD_LEGACY,
    'FAANG Experiments': STANDARD_FAANG,
    'FAANG Legacy Experiments': STANDARD_LEGACY,
    'FAANG Analyses': STANDARD_FAANG,
    'FAANG Legacy Analyses': STANDARD_LEGACY
}

# import from ena legacy
# different submitter use different terms for the same technology
# this summarize the current values observed in ENA
CATEGORIES = {
    "Whole genome sequence": "WGS",
    "whole genome sequencing": "WGS",
    "WGS": "WGS",
    "Whole Genome Shotgun Sequence": "WGS",

    "ChIP-Seq": "ChIP-Seq",
    "ChIP-seq": "ChIP-Seq",
    "ChIP-seq Histones": "ChIP-Seq",

    "Hi-C": "Hi-C",

    "ATAC-seq": "ATAC-seq",

    "RNA-Seq": "RNA-Seq",
    "RNA seq": "RNA-Seq",
    "miRNA-Seq": "miRNA",
    "ssRNA-seq": "RNA-Seq",
    "strand-specific RNA sequencing": "RNA-Seq",
    "Transcriptome profiling": "RNA-Seq",
    "RNA sequencing": "RNA-Seq",

    "Bisulfite-Seq": "BS-Seq",
    "Bisulfite Sequencing": "BS-Seq",
    "BS-Seq": "BS-Seq",
    "Whole Genome Bisulfite Sequencing": "BS-Seq",
    "WGBS": "BS-Seq",
    "Reduced Representation Bisulfite Sequencing": "BS-Seq",
    "RRBS": "BS-Seq",

    "DNase": "DNase",

    "MiSeq": "Other",
    "GeneChip": "Other",
    "MeDIP-Seq": "Other",
    "MeDIP": "Other",
    "methylated DNA immunoprecipitation-sequencing": "Other",
    "RIP-Seq": "Other"
}

EXPERIMENT_TARGETS = {
    "ATAC-seq": "open_chromatin_region",
    "BS-Seq": "DNA methylation",
    "Hi-C": "chromatin",
    "DNase": "open_chromatin_region",
    "RNA-Seq": "Unknown",
    "WGS": "input DNA",
    "ChIP-Seq": "Unknown",
    "miRNA": "Unknown",
    'CAGE-seq': "TSS"
}
