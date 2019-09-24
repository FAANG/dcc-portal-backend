import import_from_ena_legacy
from elasticsearch import Elasticsearch
# not properly structured, just to record some test cases
status, value = import_from_ena_legacy.retrieve_biosample_record("SAMN04526066")  # private
status, value = import_from_ena_legacy.retrieve_biosample_record("SAMEA104728849")  # cell culture

es = Elasticsearch()
es_index_prefix = 'faang_build_8'
# import_from_ena_legacy.retrieve_biosamples_record(es, es_index_prefix, "SAMN04526066")  # private
import_from_ena_legacy.retrieve_biosamples_record(es, es_index_prefix, "SAMEA104728849")  # cell culture
# import_from_ena_legacy.retrieve_biosamples_record(es, es_index_prefix, "SAMN11127692")  # NCBI basic
# import_from_ena_legacy.retrieve_biosamples_record(es, es_index_prefix, 'SAMEA104101910')  # EBI basic

print(import_from_ena_legacy.CACHED_MATERIAL)
