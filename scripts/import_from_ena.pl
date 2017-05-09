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

my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct');
my @ids_to_import;

my $scroll = $es->scroll_helper(
  index => $es_index_name,
  type => 'archive',
  search_type => 'scan',
  size => 500,
);
while (my $loaded_doc = $scroll->next) {
  push(@ids_to_import, $loaded_doc->{_source}{ENA}{id});
}

croak "No identifiers were availible for import" unless (@ids_to_import);

my %docs;
foreach my $ena_id (@ids_to_import){
  #ENA API DOC: http://www.ebi.ac.uk/ena/portal/api/doc?format=pdf
  my $url = "https://www.ebi.ac.uk/ena/portal/api/search?result=read_run&format=JSON&limit=0&fields=study_accession,sample_accession,experiment_alias,experiment_title,fastq_ftp,fastq_md5,last_updated&query=study_accession=".$ena_id;
  my $browser = WWW::Mechanize->new();
  $browser->credentials('anon','anon');
  $browser->get( $url );
  my $content = $browser->content();
  my $json = new JSON;
  my $json_text = $json->decode($content);
  print Dumper($json_text);


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
}

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