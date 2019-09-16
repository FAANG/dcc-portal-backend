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
    'faang_build_3_analysis': 'analysis'
}

# Current indices in use
TYPES = ['organism', 'specimen', 'file', 'experiment', 'dataset']

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
    'whole genome sequencing assay': 'WGS'
}

STANDARDS = {
    'FAANG Samples': STANDARD_FAANG,
    'FAANG Legacy Samples': STANDARD_LEGACY,
    'FAANG Experiments': STANDARD_FAANG,
    'FAANG Legacy Experiments': STANDARD_LEGACY
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
    "miRNA": "Unknown"
}
