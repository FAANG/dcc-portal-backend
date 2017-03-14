#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use WWW::Mechanize;
use JSON -support_by_pp;
use Search::Elasticsearch;
use List::Compare;
use Data::Dumper;

my ($project, $es_host);
my $es_index_name = 'faang';

GetOptions(
  'project=s' => \$project,
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
);
croak "Need -project" unless ( $project);
croak "Need -es_host" unless ( $es_host);

my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct');
my %indexed_samples;

#Sample Material storage
my %organism;
my %specimen_from_organism;
my %cell_specimen;
my %cell_culture;
my %cell_line;

# Store specimen to sample relationships for embedding search data and importing legacy organisms
my %derivedFromOrganism;

my @samples = fetch_specimens_by_project($project);

#Check that specimens were obtained from BioSamples
my $number_specimens_check = keys %specimen_from_organism;
croak "Did not obtain any specimens from BioSamples" unless ( $number_specimens_check > 0);

#Entities dependent on organism
process_specimens(%specimen_from_organism);
process_cell_specimens(%cell_specimen);
process_cell_cultures(%cell_culture);

#Independent entities
process_cell_lines(%cell_line);
process_organisms(\%organism, \%derivedFromOrganism);

sub process_specimens{
  my ( %specimen_from_organism ) = @_;
  foreach my $key (keys %specimen_from_organism){
    my $specimen = $specimen_from_organism{$key};

    #Pull in derived from accession from BioSamples.  #TODO This is slow, better way to do this?    
    my $relations = fetch_relations_json($$specimen{_links}{relations}{href});
    my $derivedFrom = fetch_relations_json($$relations{_links}{derivedFrom}{href});

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
      availability => $$specimen{characteristics}{availability}[0]{text},
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
      push(@{$es_doc{organization}}, {name => $$organization{Name}, role => $$organization{Role}, URL => $$organization{URL}});
    }
    foreach my $specimenPictureUrl (@{$$specimen{characteristics}{specimenPictureUrl}}){
      push(@{$es_doc{specimenFromOrganism}{specimenPictureUrl}}, $$specimenPictureUrl{text});
    }
    foreach my $healthStatusAtCollection (@{$$specimen{characteristics}{healthStatusAtCollection}}){
      push(@{$es_doc{specimenFromOrganism}{healthStatusAtCollection}}, {text => $$healthStatusAtCollection{text}, ontologyTerms => $$healthStatusAtCollection{ontologyTerms}[0]});
    }
    foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
      push(@{$es_doc{sameAs}}, $$sameasrelations{accession});
    }
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET  }
    if($derivedFromOrganism{$$derivedFrom{_embedded}{samplesrelations}[0]{accession}}){
      push($derivedFromOrganism{$$derivedFrom{_embedded}{samplesrelations}[0]{accession}}, \%es_doc);
    }else{
      $derivedFromOrganism{$$derivedFrom{_embedded}{samplesrelations}[0]{accession}} = [\%es_doc];
    }
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
      availability => $$specimen{characteristics}{availability}[0]{text},
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
    if($derivedFromOrganism{$$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession}}){
      push($derivedFromOrganism{$$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession}}, \%es_doc);
    }else{
      $derivedFromOrganism{$$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession}} = [\%es_doc];
    }
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
      availability => $$specimen{characteristics}{availability}[0]{text},
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
    if($derivedFromOrganism{$$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession}}){
      push($derivedFromOrganism{$$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession}}, \%es_doc);
    }else{
      $derivedFromOrganism{$$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession}} = [\%es_doc];
    }
  }
}

sub process_cell_lines{
  my ( %cell_line ) = @_;
  foreach my $key (keys %cell_line){
    my $specimen = $cell_line{$key};
    #Pull in derived from accession from BioSamples.  #TODO This is slow, better way to do this?    
    my ($relations, $derivedFrom, $derivedFrom_organism, $sameAs);
    $relations = fetch_relations_json($$specimen{_links}{relations}{href});
    $derivedFrom = fetch_relations_json($$relations{_links}{derivedFrom}{href}); #Specimen from Organism
    if($$derivedFrom{_embedded}{samplesrelations}[0]{_links}{derivedFrom}{href}){
      $derivedFrom_organism = fetch_relations_json($$derivedFrom{_embedded}{samplesrelations}[0]{_links}{derivedFrom}{href});      
    }
    
    #Pull in sameas accession from BioSamples.  #TODO This is slow, better way to do this?
    $sameAs = fetch_relations_json($$relations{_links}{sameAs}{href});

    #TODO Need cellline data section filled
    my %es_doc = (
      name => $$specimen{name},
      biosampleId => $$specimen{accession},
      description => $$specimen{description},
      material => {
        text => $$specimen{characteristics}{material}[0]{text},
        ontologyTerms => $$specimen{characteristics}{material}[0]{ontologyTerms}[0],
      },
      availability => $$specimen{characteristics}{availability}[0]{text},
      project => $$specimen{characteristics}{project}[0]{text},
      cellLine => {

      }
    );
    if($$derivedFrom{_embedded}{samplesrelations}[0]{accession}){
      $es_doc{derivedFrom} = $$derivedFrom{_embedded}{samplesrelations}[0]{accession};
    }
    foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
      push(@{$es_doc{sameAs}}, $$sameasrelations{accession});
    }
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET
    if($derivedFromOrganism{$$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession}}){
      push($derivedFromOrganism{$$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession}}, \%es_doc);
    }elsif($$derivedFrom{_embedded}{samplesrelations}[0]{accession}){
      $derivedFromOrganism{$$derivedFrom_organism{_embedded}{samplesrelations}[0]{accession}} = [\%es_doc];
    }
  }
}

sub process_organisms{
  my ( $organism_ref, $derivedFromOrganismref ) = @_;
  my %derivedFromOrganism = %$derivedFromOrganismref;
  my @obserbedOrganismList;
  my %allorganisms = %$organism_ref;
  foreach my $key (keys %allorganisms){
    my $organism = $allorganisms{$key};

    #Pull in childof accession from BioSamples.  #TODO This is slow, better way to do this?    
    my $relations = fetch_relations_json($$organism{_links}{relations}{href});
    my $childOf = fetch_relations_json($$relations{_links}{childOf}{href});

    #Pull in sameas accession from BioSamples.  #TODO This is slow, better way to do this?
    my $sameAs = fetch_relations_json($$relations{_links}{sameAs}{href});

    my %es_doc = (
      name => $$organism{name},
      biosampleId => $$organism{accession},
      description => $$organism{description},
      material => {
        text => $$organism{characteristics}{material}[0]{text},
        ontologyTerms => $$organism{characteristics}{material}[0]{ontologyTerms}[0]
      },
      availability => $$organism{characteristics}{availability}[0]{text},
      project => $$organism{characteristics}{project}[0]{text},
      organism => {
        text => $$organism{characteristics}{organism}[0]{text},
        ontologyTerms => $$organism{characteristics}{organism}[0]{ontologyTerms}[0]
      },
      sex => {
        text => $$organism{characteristics}{sex}[0]{text},
        ontologyTerms => $$organism{characteristics}{sex}[0]{ontologyTerms}[0]
      },
      breed => {
        text => $$organism{characteristics}{breed}[0]{text},
        ontologyTerms => $$organism{characteristics}{breed}[0]{ontologyTerms}[0]
      },
      birthDate => {
          text => $$organism{characteristics}{birthDate}[0]{text},
          unit => $$organism{characteristics}{birthDate}[0]{unit}
      },
      birthLocation => $$organism{characteristics}{birthLocation}[0]{text},
      birthLocationLongitude => {
          text => $$organism{characteristics}{birthLocationLongitude}[0]{text},
          unit => $$organism{characteristics}{birthLocationLongitude}[0]{unit}
      },
      birthLocationLatitude => {
          text => $$organism{characteristics}{birthLocationLatitude}[0]{text},
          unit => $$organism{characteristics}{birthLocationLatitude}[0]{unit}
      },
      birthWeight => {
          text => $$organism{characteristics}{birthWeight}[0]{text},
          unit => $$organism{characteristics}{birthWeight}[0]{unit}
      },
      placentalWeight => {
          text => $$organism{characteristics}{placentalWeight}[0]{text},
          unit => $$organism{characteristics}{placentalWeight}[0]{unit}
      },
      pregnancyLength => {
          text => $$organism{characteristics}{pregnancyLength}[0]{text},
          unit => $$organism{characteristics}{pregnancyLength}[0]{unit}
      },
      deliveryTiming => $$organism{characteristics}{deliveryTiming}[0]{text},
      deliveryEase => $$organism{characteristics}{deliveryEase}[0]{text},
      pedigree => $$organism{characteristics}{pedigree}[0]{text}
    );
    foreach my $healthStatus (@{$$organism{characteristics}{healthStatus}}){
      push(@{$es_doc{healthStatus}}, {text => $$healthStatus{text}, ontologyTerms => $$healthStatus{ontologyTerms}[0]});
    }
    foreach my $samplesrelations (@{$$childOf{_embedded}{samplesrelations}}){
      push(@{$es_doc{childOf}}, $$samplesrelations{accession});
    }
    foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
      push(@{$es_doc{sameAs}}, $$sameasrelations{accession});
    }
    push(@obserbedOrganismList, $$organism{accession});
    # standardMet => , #TODO Need to validate sample to know if standard is met, will store FAANG, LEGACY or NOTMET

    foreach my $specimen_es_doc (@{$derivedFromOrganism{$$organism{accession}}}){
      push(@{$$specimen_es_doc{organism}{biosampleId}}, $$organism{accession});
      push(@{$$specimen_es_doc{organism}{organism}}, {text => $$organism{characteristics}{organism}[0]{text}, ontologyTerms => $$organism{characteristics}{organism}[0]{ontologyTerms}[0]});
      push(@{$$specimen_es_doc{organism}{sex}}, {text => $$organism{characteristics}{sex}[0]{text}, ontologyTerms => $$organism{characteristics}{sex}[0]{ontologyTerms}[0]});
      push(@{$$specimen_es_doc{organism}{breed}}, {text => $$organism{characteristics}{breed}[0]{text}, ontologyTerms => $$organism{characteristics}{breed}[0]{ontologyTerms}[0]});
      foreach my $healthStatus (@{$$organism{characteristics}{healthStatus}}){
        push(@{$$specimen_es_doc{organism}{healthStatus}}, {text => $$healthStatus{text}, ontologyTerms => $$healthStatus{ontologyTerms}[0]});
      }
      update_elasticsearch(\%$specimen_es_doc, 'specimen');
    }
    update_elasticsearch(\%es_doc, 'organism');
  }
  my @derivedFromOrganismList = keys(%derivedFromOrganism);
  my $lc = List::Compare->new(\@derivedFromOrganismList, \@obserbedOrganismList);
  my @organismsNotImported = $lc->get_unique;
  print Dumper(@organismsNotImported) unless ( scalar(@organismsNotImported) < 1);
  croak "Have Organisms that have not been imported \@organismsNotImported" unless ( scalar(@organismsNotImported) < 1);

  #Delete removed samples
  clean_elasticsearch('specimen');
  clean_elasticsearch('organism');
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
  #$browser->show_progress(1);  # Enable for WWW::Mechanize GET logging
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
}

sub clean_elasticsearch{
  my ($type) = @_;
  my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => $type,
    search_type => 'scan',
    size => 500,
  );
  SCROLL:
  while (my $loaded_doc = $scroll->next) {
    next SCROLL if $indexed_samples{$loaded_doc->{_id}};
    $es->delete(
      index => $es_index_name,
      type => $type,
      id => $loaded_doc->{_id},
    );
  }
}