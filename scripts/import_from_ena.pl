#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Search::Elasticsearch;
use XML::Simple qw(XMLin);
use File::Basename qw(fileparse);
use URI::Escape qw(uri_escape);
use ReseqTrack::Tools::ERAUtils qw(get_erapro_conn);
use Data::Dumper;

my ($es_host, $study_id_file);
my $es_index_name = 'faang';
my @era_params = ('era_reader', undef, 'ERAPRO');

GetOptions(
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
  'era_password=s'    => \$era_params[1],
  'study_id_file=s'    => \$study_id_file,
);

croak "Need -es_host" unless ($es_host);
croak "Need -era_password" unless ($era_params[1]);

open my $fh, '<', $study_id_file;
chomp(my @study_ids = <$fh>);
close $fh;

croak "Need study IDs" unless (@study_ids);

my %indexed_files;
my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct');

my $era_db = get_erapro_conn(@era_params);
$era_db->dbc->db_handle->{LongReadLen} = 4000000;

my $sql_study =  'select xmltype.getclobval(study_xml) study_xml from study where study_id=?';
my $sth_study = $era_db->dbc->prepare($sql_study) or die "could not prepare $sql_study";

my $sql_run =  "
  select r.run_id, to_char(r.first_created, 'YYYY-MM-DD') first_created, r.experiment_id, xmltype.getclobval(r.run_xml) run_xml, s.biosample_id, e.instrument_platform, e.instrument_model, e.library_layout, e.library_strategy, e.library_source, e.library_selection, e.paired_nominal_length, xmltype.getclobval(e.experiment_xml) experiment_xml
  from sample s, run_sample rs, run r, experiment e
  where r.experiment_id=e.experiment_id and r.run_id=rs.run_id
  and s.sample_id=rs.sample_id
  and r.status_id=4
  and e.study_id=?
  ";
my $sth_run = $era_db->dbc->prepare($sql_run) or die "could not prepare $sql_run";

my %docs;
foreach my $study_id (@study_ids){
  $sth_study->bind_param(1, $study_id);
  $sth_study->execute or die "could not execute";
  my $row = $sth_study->fetchrow_hashref;
  die "no study $study_id" if !$row;
  my $study_xml_hash = XMLin($row->{STUDY_XML});

  $sth_run->bind_param(1, $study_id);
  $sth_run->execute or die "could not execute";
  ROW:
  while (my $row = $sth_run->fetchrow_hashref) {
    my $run_xml_hash = XMLin($row->{RUN_XML});
    my $experiment_xml_hash = XMLin($row->{EXPERIMENT_XML});
    my @files;
    my @specimens;
    push(@specimens, $row->{BIOSAMPLE_ID});
    foreach my $file (@{$run_xml_hash->{RUN}{DATA_BLOCK}{FILES}{FILE}}){
      my ($filename, $dirname) = fileparse($file->{filename});
      push(@files, {name => $filename, md5 => $file->{checksum}, dataType => $file->{filetype}, url => sprintf('ftp://ftp.sra.ebi.ac.uk/vol1/%s%s', $dirname, uri_escape($filename))})
    }
    
    my $es_id = join('-', $row->{EXPERIMENT_ID}, $row->{RUN_ID});
    $docs{$es_id} = {
      files => \@files,
      specimens => \@specimens
    }
  }
}

for my $es_id (keys %docs){
  eval{$es->index(
    index => $es_index_name,
    type => 'file',
    id => $es_id,
    body => $docs{$es_id},
  );};
  if (my $error = $@) {
    die "error indexing sample in $es_index_name index:".$error->{text};
  }
  $indexed_files{$es_id} = 1;
}

my $scroll = $es->scroll_helper(
  index => $es_index_name,
  type => 'file',
  search_type => 'scan',
  size => 500,
);
SCROLL:
while (my $loaded_doc = $scroll->next) {
  next SCROLL if $indexed_files{$loaded_doc->{_id}};
  $es->delete(
    index => $es_index_name,
    type => 'file',
    id => $loaded_doc->{_id},
  );
}