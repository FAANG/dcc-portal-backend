#!/bin/bash

# This script must be run as user reseq_adm because it involves rsyncing from hx to pg

perl $GCA_ELASTICSEARCH/scripts/sync_hx_hh.es.pl \
  -from ves-hx-e4 \
  -to ves-pg-e4 \
  -to ves-oy-e4 \
  -repo faang_repo \
  -snap_index faang
