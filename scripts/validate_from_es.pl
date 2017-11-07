#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Search::Elasticsearch;
use WWW::Mechanize;
use Data::Dumper;

#the library file for validation of sample records
require "validate_sample_record.pl";
#define the rulesets each record needs to be validated against, in the order of 
my @rulesets = ("FAANG Samples","FAANG Legacy Samples");
#the value for standardMet according to the ruleset, keys are expected to include all values in the @rulesets
my %standards = ("FAANG Samples"=>"FAANG","FAANG Legacy Samples"=>"FAANG Legacy");
#the elasticsearch server address
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
  #save the elasticsearch records into a hash which has keys as biosampleIDs
  my %data;
  while (my $loaded_doc = $scroll->next) {
    my $biosampleId = $$loaded_doc{_id};
#    my %data = %{$$loaded_doc{_source}};
    $data{$biosampleId} = $$loaded_doc{_source};
  }
  #validate the records against ALL specified rulesets (defined in @rulesets) by calling the method in validate_sample_record.pl
  #the returned hash has keys as rulesets, the value is another hash having three fixed key values: 
  #1. summary (how many pass, warning, or error), 
  #2. detail (validation result for every biosample record), and 
  #3. errors (all error messages and how many times that error message appeared)
  my %totalResults = &validateTotalSampleRecords(\%data,$type,\@rulesets);
  my @status = qw/pass warning error/;

  #display the validation results
  foreach my $ruleset(@rulesets){
    print "$type $ruleset Summary:\n";
    foreach (@status){
      if (exists $totalResults{$ruleset}{summary}{$_}){
        print "$_\t$totalResults{$ruleset}{summary}{$_}\n";
      }else{
        print "$_\t0\n";
      }
    }
    print "Error summary:\n";
    my %errorSummary = %{$totalResults{$ruleset}{errors}};
    foreach my $error(sort keys %errorSummary){
      print "$error\t$errorSummary{$error}\n";
    }
  }
  #Parse the details to print the error message and update the elasticsearch record
  print "$type Details:\n";
  OUTER:
  foreach my $biosampleId(sort {$a cmp $b} keys %data){
    foreach my $ruleset(@rulesets){
      next unless (exists $totalResults{$ruleset}{detail}{$biosampleId});
      if ($totalResults{$ruleset}{detail}{$biosampleId}{status} eq "error"){
        print "$biosampleId\t$totalResults{$ruleset}{detail}{$biosampleId}{type}\terror\t$ruleset\t$totalResults{$ruleset}{detail}{$biosampleId}{message}\n";
      }else{
        my %es_doc = %{$data{$biosampleId}};
        $es_doc{standardMet} = $standards{$ruleset};
        $es_doc{versionLastStandardMet} = $ruleset_version if ($es_doc{standardMet} eq "FAANG");
        eval{
          $es->index(
            index => $es_index,
            type => $type,
            id => $biosampleId,
            body => \%es_doc
          );
        };
        if (my $error = $@) {
          die "error indexing sample in $es_index index:".$error->{text};
        }
        next OUTER;
      }
    }
  }
}

