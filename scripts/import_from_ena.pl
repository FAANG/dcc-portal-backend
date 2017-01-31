#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Search::Elasticsearch;
use ReseqTrack::Tools::ERAUtils qw(get_erapro_conn);
use Data::Dumper;

my ($es_host);
my $es_index_name = 'faang';
my @era_params = ('era_reader', undef, 'ERAPRO');

GetOptions(
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
  'era_password=s'    => \$era_params[1]
  'study_id_file=s'    => \@study_id,
);

croak "Need -es_host" unless ($es_host);
croak "Need -era_password" unless ($era_params[1]);

my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct');

my $era_db = get_erapro_conn(@era_params);
$era_db->dbc->db_handle->{LongReadLen} = 4000000;

my@study_ids;

#my $scroll = $es->scroll_helper(
#  index => $es_index_name,
#  type => 'specimen',
#  search_type => 'scan',
#  size => 500,
#);
#SCROLL:
#my @era_samples;
#while (my $es_doc = $scroll->next) {
#  my $specimen_id = $$es_doc{_id};
#  my $sql_sample = "select SAMPLE_ID from SAMPLE where BIOSAMPLE_ID='$specimen_id'";
#  print Dumper($sql_sample), "\n";
#  my $sth_sample = $era_db->dbc->prepare($sql_sample) or die "could not prepare $sql_sample";
#  print Dumper($sth_sample), "\n";
#  push(@era_samples, $sth_sample);
#}
#foreach my $sample (@era_samples){
#  print $sample, "\n";
#}

