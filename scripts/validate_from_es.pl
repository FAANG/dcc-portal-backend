#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Search::Elasticsearch;
use WWW::Mechanize;
use Data::Dumper;

require "validate_sample_record.pl";


my $es_host = "ves-hx-e4:9200";
my $es_index;

GetOptions(
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index
);

croak "Need -es_host" unless ($es_host);
croak "Need -es_index_name" unless ($es_index);
print "Working on $es_index at $es_host\n";

#get the latest release version
#this requires the immediate deployment of latest release on the production server
my $ruleset_version = &getRulesetVersion();
print "Rule set release: $ruleset_version\n";

#initial ES object
my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct'); #client option to make it compatiable with elasticsearch 1.x APIs

#define what type of data to validate
my @types = qw/organism specimen/;
foreach my $type(@types){
  &validateOneType($type);
}

#validate all records in one type
sub validateOneType(){
  my $type = $_[0];
  print "\nValidating $type data\n";
  #retrieve all records from elastic search server in the chunk of 500
  my $scroll = $es->scroll_helper(
    index => $es_index,
    type => $type,
    search_type => 'scan',
    size => 500,
  );

  my %data;
  while (my $loaded_doc = $scroll->next) {
    my $biosampleId = $$loaded_doc{_id};
#    my %data = %{$$loaded_doc{_source}};
    $data{$biosampleId} = $$loaded_doc{_source};
  }

  my %totalResults;
  my $portionSize = 1000;
  my @data = keys %data;
  my $totalSize = scalar @data;
  my $numPortions = ($totalSize - $totalSize%$portionSize)/$portionSize;
  for (my $i=0;$i<$numPortions;$i++){
    my @part;
    for (my $j=0;$j<$portionSize;$j++){
      my $biosampleId = pop @data;
      push (@part,$data{$biosampleId});
    }
    my %validationResults = &validateSampleRecord(\@part,$type);
    %totalResults = %{&mergeResult(\%totalResults,\%validationResults)};
  }
  my @part;
  while(my $biosampleId = pop @data){
    push (@part,$data{$biosampleId});
  }
  my %validationResults = &validateSampleRecord(\@part,$type);
  %totalResults = %{&mergeResult(\%totalResults,\%validationResults)};
  #display the validationResults
  my @status = qw/pass warning error/;
  print "$type Summary:\n";
  foreach (@status){
    if (exists $totalResults{summary}{$_}){
      print "$_\t$totalResults{summary}{$_}\n";
    }else{
      print "$_\t0\n";
    }
  }
  print "Details:\n";
  my %details = %{$totalResults{detail}};
  foreach my $biosampleId(sort {$a cmp $b} keys %details){
    if ($details{$biosampleId}{status} eq "error"){
      print "$biosampleId\t$details{$biosampleId}{type}\terror\t$details{$biosampleId}{message}\n";
    }else{
      my %es_doc = %{$data{$biosampleId}};
      $es_doc{standardMet} = "FAANG";
      $es_doc{versionLastStandardMet} = $ruleset_version;
      eval{
        $es->index(
          index => $es_index,
          type => $details{$biosampleId}{type},
          id => $biosampleId,
          body => \%es_doc
        );
      };
      if (my $error = $@) {
        die "error indexing sample in $es_index index:".$error->{text};
      }
    }
  }
}

sub mergeResult(){
  my %totalResults = %{$_[0]};
  my %partResults = %{$_[1]};
  if (exists $totalResults{summary}){
    my @status = qw/pass warning error/;
    foreach (@status){
      if (exists $totalResults{summary}{$_} && exists $partResults{summary}{$_}){
        $totalResults{summary}{$_} += $partResults{summary}{$_};
      }elsif (exists $totalResults{summary}{$_}){
      }elsif (exists $partResults{summary}{$_}){
        $totalResults{summary}{$_} = $partResults{summary}{$_};
      }else{
        $totalResults{summary}{$_} = 0;
      }
    }
  }else{ #no summary section, means totalResults is empty (i.e. first portion of result)
    %totalResults = %partResults;
    return \%totalResults;
  }
  foreach my $tmp (keys %{$partResults{detail}}){
    $totalResults{detail}{$tmp} = $partResults{detail}{$tmp};
  }
  return \%totalResults;
}
