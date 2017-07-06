#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Search::Elasticsearch;
use WWW::Mechanize;
use JSON -support_by_pp;
use List::Compare;
use Data::Dumper;

my ($es_host, $study_id_file);
my $es_index_name = 'faang';

GetOptions(
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
);

croak "Need -es_host" unless ($es_host);

#Import FAANG data from FAANG endpoint of ENA API
#ENA API documentation available at: http://www.ebi.ac.uk/ena/portal/api/doc?format=pdf
my $url = "https://www.ebi.ac.uk/ena/portal/api/search/?result=read_run&format=JSON&limit=0&dataPortal=faang&fields=all";
my $browser = WWW::Mechanize->new();
$browser->credentials('anon','anon');
$browser->get( $url );
my $content = $browser->content();
my $json = new JSON;
my $json_text = $json->decode($content);

my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct');
my %biosample_ids;

my $scroll = $es->scroll_helper(
  index => $es_index_name,
  type => 'specimen',
  search_type => 'scan',
  size => 500,
);
while (my $loaded_doc = $scroll->next) {
  $biosample_ids{$loaded_doc->{_id}}=1;
}

croak "BioSample IDs were not imported" unless (%biosample_ids);

my %docs;

foreach my $record (@$json_text){
  if ($biosample_ids{$record->{sample_accession}}){
    my %es_doc;
    my @urls = split(";", $record->{fastq_ftp});
    my @md5s = split(";", $record->{fastq_md5});

    while (@urls){
      my @fullpath = split('/', $urls[0]);
      my $name = $fullpath[-1];
      push(@{$es_doc{files}}, {name => $name, md5 => shift(@md5s), datatype => $record->{assay_type}, url => shift(@urls)});
    }
    $es_doc{specimens => $record->{sample_accession}};
    eval{$es->index(
      index => $es_index_name,
      type => 'file',
      id => $es_doc{$record->run_accession},
      body => \%es_doc,
    );};
    if (my $error = $@) {
      die "error indexing sample in $es_index_name index:".$error->{text};
    }
    $indexed_samples{$es_doc{biosampleId}} = 1;
  }
}




#   $sth_study->bind_param(1, $study_id);
#   $sth_study->execute or die "could not execute";
#   my $row = $sth_study->fetchrow_hashref;
#   die "no study $study_id" if !$row;
#   my $study_xml_hash = XMLin($row->{STUDY_XML});

#   $sth_run->bind_param(1, $study_id);
#   $sth_run->execute or die "could not execute";
#   ROW:
#   while (my $row = $sth_run->fetchrow_hashref) {
#     my $run_xml_hash = XMLin($row->{RUN_XML});
#     my $experiment_xml_hash = XMLin($row->{EXPERIMENT_XML});
#     my @files;
#     my @specimens;
#     push(@specimens, $row->{BIOSAMPLE_ID});
#     foreach my $file (@{$run_xml_hash->{RUN}{DATA_BLOCK}{FILES}{FILE}}){
#       my ($filename, $dirname) = fileparse($file->{filename});
#       push(@files, {name => $filename, md5 => $file->{checksum}, dataType => $file->{filetype}, url => sprintf('ftp://ftp.sra.ebi.ac.uk/vol1/%s%s', $dirname, uri_escape($filename))})
#     }
    
#     my $es_id = join('-', $row->{EXPERIMENT_ID}, $row->{RUN_ID});
#     $docs{$es_id} = {
#       files => \@files,
#       specimens => \@specimens
#     }
#   }
#}

# for my $es_id (keys %docs){
#   eval{$es->index(
#     index => $es_index_name,
#     type => 'file',
#     id => $es_id,
#     body => $docs{$es_id},
#   );};
#   if (my $error = $@) {
#     die "error indexing sample in $es_index_name index:".$error->{text};
#   }
#   $indexed_files{$es_id} = 1;
# }

# my $scroll = $es->scroll_helper(
#   index => $es_index_name,
#   type => 'file',
#   search_type => 'scan',
#   size => 500,
# );
# SCROLL:
# while (my $loaded_doc = $scroll->next) {
#   next SCROLL if $indexed_files{$loaded_doc->{_id}};
#   $es->delete(
#     index => $es_index_name,
#     type => 'file',
#     id => $loaded_doc->{_id},
#   );
# }