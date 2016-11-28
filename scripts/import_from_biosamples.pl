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

# Store of derived from to track non-FAANG organisms (mainly from legacy samples)
my @derivedFromOrganismList;

my @samples = fetch_specimens_by_project($project);

#Check that specimens were obtained from BioSamples
my $number_specimens_check = keys %specimen_from_organism;
croak "Did not obtain any specimens from BioSamples" unless ( $number_specimens_check > 0);

#process_specimens(%specimen_from_organism);
#process_cell_specimens(%cell_specimen);
process_cell_cultures(%cell_culture);
#process_cell_lines(%cell_line);
#process_organisms(%organism, @derivedFromOrganismList);

sub process_specimens{
  my ( %specimen_from_organism ) = @_;
  foreach my $key (keys %specimen_from_organism){
    my $specimen = $specimen_from_organism{$key};

    #Pull in derived from accession from BioSamples.  #TODO This is slow, better way to do this?    
    my $relations = fetch_relations_json($$specimen{_links}{relations}{href});
    my $derivedFrom = fetch_relations_json($$relations{_links}{derivedFrom}{href});
    push(@derivedFromOrganismList, $$derivedFrom{_embedded}{samplesrelations}[0]{accession});
    my %es_doc = (
      name => $$specimen{name},
      biosampleId => $$specimen{accession},
      description => $$specimen{description},
      material => {
        text => $$specimen{characteristics}{material}[0]{text},
        ontologyTerms => $$specimen{characteristics}{material}[0]{ontologyTerms}[0],
      },
      availibility => $$specimen{characteristics}{availibility}[0]{text},
      project => $$specimen{characteristics}{project}[0]{text},
      derivedFrom => $$derivedFrom{_embedded}{samplesrelations}[0]{accession},
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
    foreach my $organization (@{$$specimen{organization}}){
      push(@{$es_doc{organization}}, $organization);
    }
    foreach my $specimenPictureUrl (@{$$specimen{characteristics}{specimenPictureUrl}}){
      push(@{$es_doc{specimenFromOrganism}{specimenPictureUrl}}, $$specimenPictureUrl{text});
    }
    foreach my $healthStatusAtCollection (@{$$specimen{characteristics}{healthStatusAtCollection}}){
      push(@{$es_doc{specimenFromOrganism}{healthStatusAtCollection}}, $healthStatusAtCollection);
    }
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET  }
  }
}

sub process_cell_specimens{
  my ( %cell_specimen ) = @_;
  foreach my $key (keys %cell_specimen){
    my $specimen = $cell_specimen{$key};
    
    #Pull in derived from accession from BioSamples.  #TODO This is slow, better way to do this?    
    my $relations = fetch_relations_json($$specimen{_links}{relations}{href});
    my $derivedFrom = fetch_relations_json($$relations{_links}{derivedFrom}{href}); #Specimen from Organism
    my $derivedFrom_organism = fetch_relations_json($$derivedFrom{_embedded}{samplesrelations}[0]{_links}{derivedFrom}{href});
    push(@derivedFromOrganismList, $$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession});
    
    my %es_doc = (
      name => $$specimen{name},
      biosampleId => $$specimen{accession},
      description => $$specimen{description},
      material => {
        text => $$specimen{characteristics}{material}[0]{text},
        ontologyTerms => $$specimen{characteristics}{material}[0]{ontologyTerms}[0],
      },
      availibility => $$specimen{characteristics}{availibility}[0]{text},
      project => $$specimen{characteristics}{project}[0]{text},
      derivedFrom => $$derivedFrom{_embedded}{samplesrelations}[0]{accession},
      cellSpecimen => {
        markers => $$specimen{characteristics}{markers}[0]{text},
        purificationProtocol => $$specimen{characteristics}{purificationProtocol}[0]{text},
      }
    );
    foreach my $cellType (@{$$specimen{characteristics}{cellType}}){
      push(@{$es_doc{cellSpecimen}{cellType}}, $cellType);
    }
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET
  }
}

sub process_cell_cultures{
  my ( %cell_culture ) = @_;
  foreach my $key (keys %cell_culture){
    my $specimen = $cell_culture{$key};
    #Pull in derived from accession from BioSamples.  #TODO This is slow, better way to do this?    
    my $relations = fetch_relations_json($$specimen{_links}{relations}{href});
    my $derivedFrom = fetch_relations_json($$relations{_links}{derivedFrom}{href}); #Specimen from Organism
    my $derivedFrom_organism = fetch_relations_json($$derivedFrom{_embedded}{samplesrelations}[0]{_links}{derivedFrom}{href});
    push(@derivedFromOrganismList, $$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession});

    my %es_doc = (
      name => $$specimen{name},
      biosampleId => $$specimen{accession},
      description => $$specimen{description},
      material => {
        text => $$specimen{characteristics}{material}[0]{text},
        ontologyTerms => $$specimen{characteristics}{material}[0]{ontologyTerms}[0],
      },
      availibility => $$specimen{characteristics}{availibility}[0]{text},
      project => $$specimen{characteristics}{project}[0]{text},
      derivedFrom => $$derivedFrom{_embedded}{samplesrelations}[0]{accession},
      cellCulture => {
        cultureType => {
          text => $$specimen{characteristics}{cultureType}[0]{text},
          ontologyTerms => $$specimen{characteristics}{cultureType}[0]{ontologyTerms}[0],
        },
        cellType => {
          text => $$specimen{characteristics}{cellType}[0]{text},
          ontologyTerms => $$specimen{characteristics}{cellType}[0]{ontologyTerms}[0],
        },
        cellCultureProtocol => $$specimen{characteristics}{cellCultureProtocol}[0]{text},
        cultureConditions => $$specimen{characteristics}{cultureConditions}[0]{text},
        NumberOfPassages => $$specimen{characteristics}{numberOfPassages}[0]{text},
      }
    );
    print Dumper(%es_doc), "\n\n\n\n\n";
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET
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
  
  while ($$json_text{_links}{next}{href}){  # Iterate until no more pages using HAL links
    $browser->get( $$json_text{_links}{next}{href});  # Get next page
    $content = $browser->content();
    $json_text = $json->decode($content);
    foreach my $item ($json_text->{_embedded}){
      push(@pages, $item);  # Store each additional page
    }
  }
  return @pages;
}

sub fetch_relations_json{
  my ($json_url) = @_;

  my $browser = WWW::Mechanize->new();
  $browser->get( $json_url );
  my $content = $browser->content();
  my $json = new JSON;
  my $json_text = $json->decode($content);
  return $json_text;
}