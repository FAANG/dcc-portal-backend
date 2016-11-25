#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use WWW::Mechanize;
use JSON -support_by_pp;
use Data::Dumper;

my $project;
GetOptions(
  "project=s" => \$project
);
croak "Need -project" unless ( $project);

#Sample Material storage
my %organism;
my %specimen_from_organism;
my %cell_specimen;
my %cell_culture;
my %cell_line;

my @samples = fetch_specimens_by_project($project);

#Check that specimens were obtained from BioSamples
my $number_specimens_check = keys %specimen_from_organism;
croak "Did not obtain any specimens from BioSamples" unless ( $number_specimens_check > 0);

process_specimens(%specimen_from_organism);

sub process_specimens{
  my ( %specimen_from_organism ) = @_;
  foreach my $key (keys %specimen_from_organism){
    my $specimen = $specimen_from_organism{$key};
    
    my %es_doc = (
      name => $$specimen{name},
      biosampleId => $$specimen{accession},
      description => $$specimen{description},
#      standardMet => , #TODO Need to validate sample to know if standard is met
      material => {
        text => $$specimen{characteristics}{material}[0]{text},
        ontologyTerms => $$specimen{characteristics}{material}[0]{ontologyTerms}[0],
      },
      availibility => $$specimen{characteristics}{availibility}[0]{text},
      specimenFromOrganism => {
        specimenCollectionDate => {
          text => $$specimen{characteristics}{specimenCollectionDate}[0]{text},
          unit => $$specimen{characteristics}{specimenCollectionDate}[0]{unit}
        },
        animalAgeAtCollection => {
          text => $$specimen{characteristics}{animalAgeAtCollection}[0]{text},
          unit => $$specimen{characteristics}{animalAgeAtCollection}[0]{unit}
        },
        developmentalStage => {
          text => $$specimen{characteristics}{developmentalStage}[0]{text},
          ontologyTerms => $$specimen{characteristics}{developmentalStage}[0]{ontologyTerms}[0]
        },
        organismPart => {
          text => $$specimen{characteristics}{organismPart}[0]{text},
          ontologyTerms => $$specimen{characteristics}{organismPart}[0]{ontologyTerms}[0]
        },
        SpecimenCollectionProtocol => $$specimen{characteristics}{specimenCollectionProtocol}[0]{text},
        fastedStatus => $$specimen{characteristics}{fastedStatus}[0]{text},
        numberOfPieces => {
          text => $$specimen{characteristics}{numberOfPieces}[0]{text},
          unit => $$specimen{characteristics}{numberOfPieces}[0]{unit}
        },
        specimenVolume => {
          text => $$specimen{characteristics}{specimenVolume}[0]{text},
          unit => $$specimen{characteristics}{specimenVolume}[0]{unit}
        },
        specimenSize => {
          text => $$specimen{characteristics}{specimenSize}[0]{text},
          unit => $$specimen{characteristics}{specimenSize}[0]{unit}
        },
        specimenWeight => {
          text => $$specimen{characteristics}{specimenWeight}[0]{text},
          unit => $$specimen{characteristics}{specimenWeight}[0]{unit}
        }
      }
    );
    
    #Pull in derived from accession
    #my derivedURL = "https://www.ebi.ac.uk/biosamples/api/samplesrelations/".$$specimen{accession}."/derivedFrom"
#   derivedFrom =>
    #"derivedFrom" : {
    #"href" : "https://www.ebi.ac.uk/biosamples/api/samplesrelations/SAMEA6270418/derivedFrom"

    #TODO Decide whether to pull in parent information such as organism

    #TODO Add existing parents to list to ensure that non FAANG parents are imported

    #Add items that can have multiples
    #organization => { #TODO Can have multiple orgs?
#        name => ,
#        role => ,
#        URL => ,
#      },
#        specimenPictureUrl => $specimen{specimenPictureUrl}[0]{text} #TODO Can be multiple
#        healthStatusAtCollection => {
#          text =>
#          ontologyTerms =>
#        },
  }
}

sub fetch_specimens_by_project {
  my ( $project_keyword ) = @_;
  
  my $url = "https://www.ebi.ac.uk/biosamples/api/samples/search/findByText?text=project_crt:".$project_keyword;
  my @pages = fetch_biosamples_json($url);
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