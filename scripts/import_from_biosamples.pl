#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use WWW::Mechanize;
use JSON -support_by_pp;
use Search::Elasticsearch;
use Data::Dumper;

my ($project, $es_host);
my $es_index_name = 'faang';

GetOptions(
  'project=s' => \$project,
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
);
croak "Need -project" unless ( $project);

my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct');
my %indexed_samples;

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

#Entities dependent on organism
process_specimens(%specimen_from_organism);
process_cell_specimens(%cell_specimen);
process_cell_cultures(%cell_culture);

#Independent entities
process_cell_lines(%cell_line); #TODO Need to know how organism, sex and breed is stored
process_organisms(\%organism, \@derivedFromOrganismList);

sub process_specimens{
  my ( %specimen_from_organism ) = @_;
  foreach my $key (keys %specimen_from_organism){
    my $specimen = $specimen_from_organism{$key};

    #Pull in derived from accession from BioSamples.  #TODO This is slow, better way to do this?    
    my $relations = fetch_relations_json($$specimen{_links}{relations}{href});
    my $derivedFrom = fetch_relations_json($$relations{_links}{derivedFrom}{href});
    push(@derivedFromOrganismList, $$derivedFrom{_embedded}{samplesrelations}[0]{accession});

    #Pull in sameas accession from BioSamples.  #TODO This is slow, better way to do this?
    my $sameAs = fetch_relations_json($$relations{_links}{sameAs}{href});

    my %es_doc = (
      name => $$specimen{name},
      biosampleId => $$specimen{accession},
      description => $$specimen{description},
      material => {
        text => $$specimen{characteristics}{material}[0]{text},
        ontologyTerms => $$specimen{characteristics}{material}[0]{ontologyTerms}[0]
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
        specimenCollectionProtocol => $$specimen{characteristics}{specimenCollectionProtocol}[0]{text},
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
    foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
      push(@{$es_doc{sameAs}}, $$sameasrelations{accession});
    }
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET  }
    update_elasticsearch(\%es_doc, 'specimen');
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

    #Pull in sameas accession from BioSamples.  #TODO This is slow, better way to do this?
    my $sameAs = fetch_relations_json($$relations{_links}{sameAs}{href});
    
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
    foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
      push(@{$es_doc{sameAs}}, $$sameasrelations{accession});
    }
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET
    update_elasticsearch(\%es_doc, 'specimen');
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

    #Pull in sameas accession from BioSamples.  #TODO This is slow, better way to do this?
    my $sameAs = fetch_relations_json($$relations{_links}{sameAs}{href});

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
        numberOfPassages => $$specimen{characteristics}{numberOfPassages}[0]{text},
      }
    );
    foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
      push(@{$es_doc{sameAs}}, $$sameasrelations{accession});
    }
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET
    update_elasticsearch(\%es_doc, 'specimen');
  }
}

sub process_cell_lines{
  my ( %cell_line ) = @_;
  foreach my $key (keys %cell_line){
    my $specimen = $cell_line{$key};
    #Pull in derived from accession from BioSamples.  #TODO This is slow, better way to do this?    
    my $relations = fetch_relations_json($$specimen{_links}{relations}{href});
    my $derivedFrom = fetch_relations_json($$relations{_links}{derivedFrom}{href}); #Specimen from Organism
    my $derivedFrom_organism = fetch_relations_json($$derivedFrom{_embedded}{samplesrelations}[0]{_links}{derivedFrom}{href});
    push(@derivedFromOrganismList, $$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession});
    
    #Pull in sameas accession from BioSamples.  #TODO This is slow, better way to do this?
    my $sameAs = fetch_relations_json($$relations{_links}{sameAs}{href});

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
      cellLine => {

      }
    );
    foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
      push(@{$es_doc{sameAs}}, $$sameasrelations{accession});
    }
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET
    update_elasticsearch(\%es_doc, 'specimen');
  }
}

sub process_organisms{
  my ( $organism_ref, $derivedFromOrganismListref ) = @_;
  my @derivedFromOrganismList = @$derivedFromOrganismListref;
  my %organism = %$organism_ref;
  foreach my $key (keys %organism){
    my $specimen = $organism{$key};

    #Pull in childof accession from BioSamples.  #TODO This is slow, better way to do this?    
    my $relations = fetch_relations_json($$specimen{_links}{relations}{href});
    my $childOf = fetch_relations_json($$relations{_links}{childOf}{href});

    #Pull in sameas accession from BioSamples.  #TODO This is slow, better way to do this?
    my $sameAs = fetch_relations_json($$relations{_links}{sameAs}{href});

    my %es_doc = (
      name => $$specimen{name},
      biosampleId => $$specimen{accession},
      description => $$specimen{description},
      material => {
        text => $$specimen{characteristics}{material}[0]{text},
        ontologyTerms => $$specimen{characteristics}{material}[0]{ontologyTerms}[0]
      },
      availibility => $$specimen{characteristics}{availibility}[0]{text},
      project => $$specimen{characteristics}{project}[0]{text},
      organism => {
        text => $$specimen{characteristics}{organism}[0]{text},
        ontologyTerms => $$specimen{characteristics}{organism}[0]{ontologyTerms}[0]
      },
      sex => {
        text => $$specimen{characteristics}{sex}[0]{text},
        ontologyTerms => $$specimen{characteristics}{sex}[0]{ontologyTerms}[0]
      },
      breed => {
        text => $$specimen{characteristics}{breed}[0]{text},
        ontologyTerms => $$specimen{characteristics}{breed}[0]{ontologyTerms}[0]
      },
      birthDate => {
          text => $$specimen{characteristics}{birthDate}[0]{text},
          unit => $$specimen{characteristics}{birthDate}[0]{unit}
      },
      birthLocation => $$specimen{characteristics}{birthLocation}[0]{text},
      birthLocationLongitude => {
          text => $$specimen{characteristics}{birthLocationLongitude}[0]{text},
          unit => $$specimen{characteristics}{birthLocationLongitude}[0]{unit}
      },
      birthLocationLatitude => {
          text => $$specimen{characteristics}{birthLocationLatitude}[0]{text},
          unit => $$specimen{characteristics}{birthLocationLatitude}[0]{unit}
      },
      birthWeight => {
          text => $$specimen{characteristics}{birthWeight}[0]{text},
          unit => $$specimen{characteristics}{birthWeight}[0]{unit}
      },
      placentalWeight => {
          text => $$specimen{characteristics}{placentalWeight}[0]{text},
          unit => $$specimen{characteristics}{placentalWeight}[0]{unit}
      },
      pregnancyLength => {
          text => $$specimen{characteristics}{pregnancyLength}[0]{text},
          unit => $$specimen{characteristics}{pregnancyLength}[0]{unit}
      },
      deliveryTiming => $$specimen{characteristics}{deliveryTiming}[0]{text},
      deliveryEase => $$specimen{characteristics}{deliveryEase}[0]{text},
      pedigree => $$specimen{characteristics}{pedigree}[0]{text}
    );
    foreach my $healthStatus (@{$$specimen{characteristics}{healthStatus}}){
      push(@{$es_doc{healthStatus}}, $healthStatus);
    }
    foreach my $samplesrelations (@{$$childOf{_embedded}{samplesrelations}}){
      push(@{$es_doc{childOf}}, $$samplesrelations{accession});
    }
    foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
      push(@{$es_doc{sameAs}}, $$sameasrelations{accession});
    }
    #TODO do something with @derivedFromOrganismList to check whether all required organisms have been imported
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET
    update_elasticsearch(\%es_doc, 'organism');
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

sub update_elasticsearch{
  my ($es_doc_ref, $type) = @_;
  my %es_doc = %$es_doc_ref;
  eval{$es->index(
    index => $es_index_name,
    type => $type,
    id => $es_doc{biosampleId},
    body => \%es_doc,
  );};
  if (my $error = $@) {
    die "error indexing sample in $es_index_name index:".$error->{text};
  }
  $indexed_samples{$es_doc{biosampleId}} = 1;

  my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => $type,
    search_type => 'scan',
    size => 500,
  );
  SCROLL:
  while (my $es_doc = $scroll->next) {
    next SCROLL if $indexed_samples{$es_doc->{_id}};
    $es->delete(
      index => $es_index_name,
      type => $type,
      id => $es_doc->{_id},
    );
  }
}