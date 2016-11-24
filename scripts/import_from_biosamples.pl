#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Data::Dumper;
use WWW::Mechanize;
use JSON -support_by_pp;

my ($project);

GetOptions(
  "project=s" => \$project
);

croak "Need -project" unless ( $project);

my @samples = fetch_specimens_by_project($project);

#Material storage
my %organism;
my %specimen_from_organism;
my %cell_specimen;
my %cell_culture;
my %cell_line;

my $counter = 1;

sub fetch_specimens_by_project {
  my ( $project_keyword ) = @_;
  
  my $url = "https://www.ebi.ac.uk/biosamples/api/samples/search/findByText?text=project_crt:".$project_keyword;
  
  my @pages = fetch_biosamples_json($url);
  my $specimens;
  foreach my $page (@pages){
    foreach my $sample (@{$page->{samples}}){
      my $material = $$sample{characteristics}{material}[0]{text};
      if($material eq "organism"){
        $organism{$$sample{accession}} = $sample;
      }
      if($material eq "specimen from organism"){
        $specimen_from_organism{$$sample{accession}} = $sample;
      }
      if($material eq "cell specimen"){
        $cell_specimen{$$sample{accession}} = $sample;
      }
      if($material eq "cell culture"){
        $cell_culture{$$sample{accession}} = $sample;
      }
      if($material eq "cell line"){
        $cell_line{$$sample{accession}} = $sample;
      }
    }
  }
}

#Check that specimens were obtained from BioSamples
my $number_specimens_check = keys %specimen_from_organism;
croak "Did not obtain any specimens from BioSamples" unless ( $number_specimens_check > 0);



sub fetch_biosamples_json{
  my ($json_url) = @_;

  my $browser = WWW::Mechanize->new();
  $browser->get( $json_url );
  my $content = $browser->content();
  my $json = new JSON;
  my $json_text = $json->decode($content);
  
  my @pages;
  foreach my $item ($json_text->{_embedded}){ #Store page 0
    push(@pages, $item);
  }
  
  while ($$json_text{_links}{next}{href}){  # Iterate until no more pages
    $browser->get( $$json_text{_links}{next}{href});  # Get next page
    $content = $browser->content();
    $json_text = $json->decode($content);
    foreach my $item ($json_text->{_embedded}){
      push(@pages, $item);  # Store each page
    }
  }
  return @pages;
}