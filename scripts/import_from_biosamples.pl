#!/usr/bin/env perl
#It is strongly recommended to read this code while referring to the sample ruleset http://www.ebi.ac.uk/vg/faang/rule_sets/FAANG%20Samples
#similar to referring to corresponding xsd file while writing codes for parsing the xml file
#e.g. whether use array or hash depends on cardinality
#e.g. the description of deriveFrom and sameAs could determine how to deal with relationship

use strict;
use warnings;
use Getopt::Long;
use Carp;
use WWW::Mechanize;
use JSON -support_by_pp;
use Search::Elasticsearch;
use List::Compare;
use Data::Dumper;
#the library file for validation of sample records
require "validate_sample_record.pl";

#the code to test getFilenameFromURL
#my $url = "http://www.ncbi.nlm.nih.gov/pubmed/16215741";
#$url = "ftp://ftp.faang.ebi.ac.uk/ftp/protocols/samples/KU_Pool_creation_protocol_20170523.pdf";
#my $filename = &getFilenameFromURL($url);
#print "$filename\n";

#the code to test the judgement for type of derived from for cell line
#as currently no data in cell line, use specimen from organism to do a fake test
#my $accession = "SAMEA3540916"; #only cell line
#my $accession = "SAMEA5584168"; #specimen from organism
#my %tmp = &fetch_single_record($accession);
#my $spec = $tmp{$accession};
#my %relationships = %{&parseRelationships($$spec{_links}{relations}{href},3)};
#print Dumper(\%relationships);
#exit;

#the parameters expected to be retrieved from the command line
my ($project, $es_host, $es_index_name, $error_log);
$error_log = "import_biosample_error.log";
#Parse the command line options
#Example: perl import_from_biosamples.pl -project faang -es_host <elasticsearch server> -es_index_name faang
GetOptions(
  'project=s' => \$project,
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
  'error_log=s' =>\$error_log
);
croak "Need -project e.g. faang" unless ( $project);
croak "Need -es_host e.g. ves-hx-e4:9200" unless ( $es_host);
croak "Need -es_index_name e.g. faang, faang_build_1" unless ( $es_index_name);

print "The information of invalid records will be stored in $error_log\n\n";
open ERR,">$error_log";

#define the rulesets each record needs to be validated against, in the order of 
my @rulesets = ("FAANG Samples","FAANG Legacy Samples");
#the value for standardMet according to the ruleset, keys are expected to include all values in the @rulesets
my %standards = ("FAANG Samples"=>"FAANG","FAANG Legacy Samples"=>"FAANG Legacy");
my $ruleset_version = &getRulesetVersion();
print "Rule set release: $ruleset_version\n";

my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct');#client option to make it compatiable with elasticsearch 1.x APIs

my %indexed_samples;

#Sample Material storage
my %organism;
my %specimen_from_organism;
my %cell_specimen;
my %cell_culture;
my %cell_line;
my %pool_specimen;
#the data structure stores information which needs to be added to specimen as organism section
my %organismInfoForSpecimen;
#keys are organism accession and values are how many specimens related to that organism
my %organismReferredBySpecimen;

#print "The program starts at ".localtime."\n";
my $pastTime = time;
my $savedTime = time;
##################################################################
## the section below is for development purpose by checking individual BioSample instead of reading all entries from BioSample
#my %tmp = &fetch_single_record("SAMEA3540911"); #pool specimen
#&process_pool_specimen(%tmp);
#my %tmp = &fetch_single_record("SAMEA4447551"); #cell culture
#&process_cell_cultures(%tmp);
#my %tmp = &fetch_single_record("SAMEA5421418"); #cell specimen
#&process_cell_specimens(%tmp);
#my %tmp = &fetch_single_record("SAMEA5584168"); #specimen from organism
#&process_specimens(%tmp);
#my %tmp = &fetch_single_record("SAMEA3540916"); #cell line
#&process_cell_lines(\%tmp);
#exit;
##################################################################

#retrieve all FAANG BioSamples from BioSample database
my @samples = fetch_specimens_by_project($project);
#print "Finish retrieving data from BioSample at ".localtime."\n";
################################################################
## this section is to dump all current BioSample records into files 
## for development purpose, e.g. check the data structure
#print "Pool of specimen\n";
#print Dumper (\%pool_specimen);
#print "Cell specimen\n";
#print Dumper (\%cell_specimen);
#print "Cell culture\n";
#print Dumper (\%cell_culture);
#print "Cell line\n";
#print Dumper (\%cell_line);
#print Dumper (\%organism);
#exit;

my $current = time;
#&convertSeconds($current - $pastTime);
$pastTime = $current;

#Check whether specimens were obtained from BioSamples
my $number_specimens_check = keys %specimen_from_organism;
croak "Did not obtain any specimens from BioSamples" unless ( $number_specimens_check > 0);
#organism needs to be processed first which populates %organismInfoForSpecimen
#print "Indexing organism starts at ".localtime."\n";
&process_organisms(\%organism);
$current = time;
#&convertSeconds($current - $pastTime);
$pastTime = $current;
#parse all types of specimen, at the moment the order does not matter
#print "Indexing specimen from organism starts at ".localtime."\n";
&process_specimens(\%specimen_from_organism);
$current = time;
#&convertSeconds($current - $pastTime);
$pastTime = $current;

#print "Indexing cell specimens starts at ".localtime."\n";
&process_cell_specimens(\%cell_specimen);
$current = time;
#&convertSeconds($current - $pastTime);
$pastTime = $current;

#print "Indexing cell culture starts at ".localtime."\n";
&process_cell_cultures(\%cell_culture);
$current = time;
#&convertSeconds($current - $pastTime);
$pastTime = $current;

#print "Indexing pool of specimen starts at ".localtime."\n";
&process_pool_specimen(\%pool_specimen);
$current = time;
#&convertSeconds($current - $pastTime);
$pastTime = $current;

#print "Indexing cell line starts at ".localtime."\n";
&process_cell_lines(\%cell_line);
$current = time;
#&convertSeconds($current - $pastTime);
$pastTime = $current;

#print "Finish indexing into ElasticSearch at ".localtime."\n";

#compare the list of all organisms and organisms with specimen
#if they differ, could be 1) organism without specimen 2) organism with specimen is not marked as organism or not saved in BioSample
my @allOrganismList = keys(%organism);
my @organismReferredList = keys %organismReferredBySpecimen;
#List::Compare - Compare elements of two or more lists
my $lc = List::Compare->new(\@allOrganismList, \@organismReferredList);
my @organismsNotImported = $lc->get_unique;
unless ( scalar(@organismsNotImported) < 1){
  print Dumper(@organismsNotImported);
  croak "Have Organisms that have not been imported \@organismsNotImported";
}

#Delete removed samples
clean_elasticsearch('specimen');
clean_elasticsearch('organism');
$current = time;
#print "Total ";
#&convertSeconds($current - $savedTime);
#print "The program ends at ".localtime."\n";

#process specimen from organism
sub process_specimens{
  my %specimen_from_organism = %{$_[0]};
  my %converted;
  foreach my $key (keys %specimen_from_organism){
    my $specimen = $specimen_from_organism{$key};

    my %relationships = %{&parseRelationships($$specimen{_links}{relations}{href},1)};

    my $url = $$specimen{characteristics}{specimenCollectionProtocol}[0]{text};
    my $filename = &getFilenameFromURL($url);

    my %es_doc = (
      derivedFrom => $relationships{derivedFrom}[0],
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
#        specimenCollectionProtocol => $$specimen{characteristics}{specimenCollectionProtocol}[0]{text},
        specimenCollectionProtocol => {
          url => $url,
          filename => $filename
        },
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
        },
        gestationalAgeAtSampleCollection => {
          text => $$specimen{characteristics}{gestationalAgeAtSampleCollection}[0]{text},
          unit => $$specimen{characteristics}{gestationalAgeAtSampleCollection}[0]{unit}
        }
      }
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$specimen)};
    $es_doc{cellType} = {text => $$specimen{characteristics}{organismPart}[0]{text}, ontologyTerms => $$specimen{characteristics}{organismPart}[0]{ontologyTerms}[0]};

    foreach my $specimenPictureUrl (@{$$specimen{characteristics}{specimenPictureUrl}}){
      push(@{$es_doc{specimenFromOrganism}{specimenPictureUrl}}, $$specimenPictureUrl{text});
    }
    foreach my $healthStatusAtCollection (@{$$specimen{characteristics}{healthStatusAtCollection}}){
      push(@{$es_doc{specimenFromOrganism}{healthStatusAtCollection}}, {text => $$healthStatusAtCollection{text}, ontologyTerms => $$healthStatusAtCollection{ontologyTerms}[0]});
    }
    @{$es_doc{sameAs}} = @{$relationships{sameAs}} if (exists $relationships{sameAs});
    my $organismAccession = $relationships{organism}[0];
    $es_doc{organism}=$organismInfoForSpecimen{$organismAccession};
    $organismReferredBySpecimen{$organismAccession}++;
    #the validation service can only be applied on FAANG data, not BioSample data
    #therefore it needs to be converted first using the codes above, and save into %converted
    #it should be more efficient to validate multiple records than one record at a time
    %{$converted{$key}} = %es_doc;
  }
  #only insert validated entries
  &insert_into_es(\%converted,"specimen");
}

sub process_pool_specimen{
  my %pool_specimen = %{$_[0]};
  my %converted;
  foreach my $accession(keys %pool_specimen){
    my $specimen = $pool_specimen{$accession};#e.g. accession = SAMEA3540911
    my %relationships = %{&parseRelationships($$specimen{_links}{relations}{href},2)};

    my $url = $$specimen{characteristics}{poolCreationProtocol}[0]{text};
    my $filename = &getFilenameFromURL($url);

    my %es_doc = (
#      sameAs => ,     #according to ruleset, it should be single value entry, i.e. use a hash. However for all other types, an array is used, to make it consistent, use array here as well
      poolOfSpecimens => {
        poolCreationDate => {
          text => $$specimen{characteristics}{poolCreationDate}[0]{text},
          unit => $$specimen{characteristics}{poolCreationDate}[0]{unit}
        },
        poolCreationProtocol => {
          url => $url,
          filename => $filename
        },
        specimenVolume => {     #no example in the current FAANG collection for pool of specimens, pure guess, expect to change later when data becomes available
          text => $$specimen{characteristics}{specimenVolume}[0]{text},
          unit => $$specimen{characteristics}{specimenVolume}[0]{unit}
        },
        specimenSize => {       #no example in the current FAANG collection for pool of specimens, pure guess, expect to change later when data becomes available
          text => $$specimen{characteristics}{specimenSize}[0]{text},
          unit => $$specimen{characteristics}{specimenSize}[0]{unit}
        },
        specimenWeight => {     #no example in the current FAANG collection for pool of specimens, pure guess, expect to change later when data becomes available
          text => $$specimen{characteristics}{specimenWeight}[0]{text},
          unit => $$specimen{characteristics}{specimenWeight}[0]{unit}
        }
      }
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$specimen)};
    $es_doc{cellType} = {text => "Not Applicable"};

    foreach my $spu (@{$$specimen{characteristics}{specimenPictureUrl}}){ #no example in the current FAANG collection for pool of specimens, pure guess, expect to change later when data becomes available
      push (@{$es_doc{poolOfSpecimens}{specimenPictureUrl}},$$spu{text});
    }
    @{$es_doc{derivedFrom}} = @{$relationships{derivedFrom}} if (exists $relationships{derivedFrom});
    @{$es_doc{sameAs}} = @{$relationships{sameAs}} if (exists $relationships{sameAs});
    #at the moment, organism information is not stored as it can be retrieved from each individual derivedFrom specimen
    #if in future it is needed, e.g. to reduce calculation time for frontend, the logic should be placed in the foreach loop below
    my %tmp;
    foreach my $organismAccession(@{$relationships{organism}}){
      $organismReferredBySpecimen{$organismAccession}++;
      $tmp{organism}{$organismInfoForSpecimen{$organismAccession}{organism}{text}} = $organismInfoForSpecimen{$organismAccession}{organism}{ontologyTerms};
      $tmp{sex}{$organismInfoForSpecimen{$organismAccession}{sex}{text}} = $organismInfoForSpecimen{$organismAccession}{sex}{ontologyTerms};
      $tmp{breed}{$organismInfoForSpecimen{$organismAccession}{breed}{text}} = $organismInfoForSpecimen{$organismAccession}{breed}{ontologyTerms};
    }
    #assign the collective value of organism, sex and breed for all related specimen
    my @arr = ("organism","sex","breed");
    foreach my $type(@arr){
      my @values = keys %{$tmp{$type}};
      if (scalar @values == 1){
        $es_doc{organism}{$type}{text} = $values[0];
        $es_doc{organism}{$type}{ontologyTerms} = $tmp{$type}{$values[0]};
      }else{
        $es_doc{organism}{$type}{text} = join (";", @values);
      }
    }
    %{$converted{$accession}} = %es_doc;
  }
  &insert_into_es(\%converted,"specimen");
}

sub process_cell_specimens{
  my %cell_specimen = %{$_[0]};
  my %converted;
  foreach my $key (keys %cell_specimen){
    my $specimen = $cell_specimen{$key};
    my %relationships = %{&parseRelationships($$specimen{_links}{relations}{href},2)};
    
    my $url = $$specimen{characteristics}{purificationProtocol}[0]{text};
    my $filename = &getFilenameFromURL($url);

    my %es_doc = (
      derivedFrom => $relationships{derivedFrom}[0],
      cellSpecimen => {
        markers => $$specimen{characteristics}{markers}[0]{text},
        purificationProtocol => {
          url => $url,
          filename => $filename
        }        
      }
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$specimen)};
    $es_doc{cellType} = {text => $$specimen{characteristics}{cellType}[0]{text}, ontologyTerms => $$specimen{characteristics}{cellType}[0]{ontologyTerms}[0]};

    foreach my $cellType (@{$$specimen{characteristics}{cellType}}){
      push(@{$es_doc{cellSpecimen}{cellType}}, $cellType);
    }
    @{$es_doc{sameAs}} = @{$relationships{sameAs}} if (exists $relationships{sameAs});
    my $organismAccession = $relationships{organism}[0];
    $es_doc{organism}=$organismInfoForSpecimen{$organismAccession};
    $organismReferredBySpecimen{$organismAccession}++;
    %{$converted{$key}} = %es_doc;
  }
  &insert_into_es(\%converted,"specimen");
}

sub process_cell_cultures{
  my %cell_culture = %{$_[0]};
  my %converted;
  foreach my $key (keys %cell_culture){
    my $specimen = $cell_culture{$key};
    my %relationships = %{&parseRelationships($$specimen{_links}{relations}{href},2)};

    my $url = $$specimen{characteristics}{cellCultureProtocol}[0]{text};
    my $filename = &getFilenameFromURL($url);

    my %es_doc = (
      derivedFrom => $relationships{derivedFrom}[0],
      cellCulture => {
        cultureType => {
          text => $$specimen{characteristics}{cultureType}[0]{text},
          ontologyTerms => $$specimen{characteristics}{cultureType}[0]{ontologyTerms}[0],
        },
        cellType => {
          text => $$specimen{characteristics}{cellType}[0]{text},
          ontologyTerms => $$specimen{characteristics}{cellType}[0]{ontologyTerms}[0],
        },
        cellCultureProtocol => {
          url => $url,
          filename => $filename
        },
        cultureConditions => $$specimen{characteristics}{cultureConditions}[0]{text},
        numberOfPassages => $$specimen{characteristics}{numberOfPassages}[0]{text}
      }
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$specimen)};
    $es_doc{cellType} = {text => $$specimen{characteristics}{cellType}[0]{text}, ontologyTerms => $$specimen{characteristics}{cellType}[0]{ontologyTerms}[0]};

    @{$es_doc{sameAs}} = @{$relationships{sameAs}} if (exists $relationships{sameAs});
    my $organismAccession = $relationships{organism}[0];
    $es_doc{organism}=$organismInfoForSpecimen{$organismAccession};
    $organismReferredBySpecimen{$organismAccession}++;
    %{$converted{$key}} = %es_doc;
  }
  &insert_into_es(\%converted,"specimen");
}

sub process_cell_lines{
  my %cell_line = %{$_[0]};
  my %converted;
  foreach my $key (keys %cell_line){
    my $specimen = $cell_line{$key};
    my %relationships = %{&parseRelationships($$specimen{_links}{relations}{href},3)};

    my $url;
    my $filename ;
    if (exists $$specimen{characteristics}{cultureProtocol}[0]{text}){
      $url = $$specimen{characteristics}{cultureProtocol}[0]{text};
      $filename = &getFilenameFromURL($url);
    }
    my %es_doc = (
      cellLine => {
        organism => {
          text => $$specimen{characteristics}{organism}[0]{text},
          ontologyTerms => $$specimen{characteristics}{organism}[0]{ontologyTerms}[0]
        },
        sex => {
          text => $$specimen{characteristics}{sex}[0]{text},
          ontologyTerms => $$specimen{characteristics}{sex}[0]{ontologyTerms}[0]
        },
        cellLine => $$specimen{characteristics}{cellLine}[0]{text},
        biomaterialProvider => $$specimen{characteristics}{biomaterialProvider}[0]{text},
        catalogueNumber => $$specimen{characteristics}{catalogueNumber}[0]{text},
        numberOfPassages => $$specimen{characteristics}{numberOfPassages}[0]{text},
        dateEstablished => { 
          text => $$specimen{characteristics}{dateEstablished}[0]{text},
          unit => $$specimen{characteristics}{dateEstablished}[0]{unit}
        },
        publication => $$specimen{characteristics}{publication}[0]{text},
        breed => {
          text => $$specimen{characteristics}{breed}[0]{text},
          ontologyTerms => $$specimen{characteristics}{breed}[0]{ontologyTerms}[0]
        },
        cellType => {
          text => $$specimen{characteristics}{cellType}[0]{text},
          ontologyTerms => $$specimen{characteristics}{cellType}[0]{ontologyTerms}[0]
        },
        cultureConditions => $$specimen{characteristics}{cultureConditions}[0]{text},
        cultureProtocol => {
          url => $url,
          filename => $filename
        },
        disease => {
          text => $$specimen{characteristics}{disease}[0]{text},
          ontologyTerms => $$specimen{characteristics}{disease}[0]{ontologyTerms}[0]
        },
        karyotype => $$specimen{characteristics}{karyotype}[0]{text}
      }
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$specimen)};
    $es_doc{cellType} = {text => $$specimen{characteristics}{cellType}[0]{text}, ontologyTerms => $$specimen{characteristics}{cellType}[0]{ontologyTerms}[0]};

    $es_doc{derivedFrom} = $relationships{derivedFrom}[0] if (exists $relationships{derivedFrom});
    @{$es_doc{sameAs}} = @{$relationships{sameAs}} if (exists $relationships{sameAs});
    if (exists $relationships{organism} && (scalar @{$relationships{organism}})>0){ #derive from an animal, use animal as its organism
      my $organismAccession = $relationships{organism}[0];
      $es_doc{organism}=$organismInfoForSpecimen{$organismAccession};
      $organismReferredBySpecimen{$organismAccession}++;
    }else{ #derive from specimen, use the cellLine organism info
      my @arr = ("organism","sex","breed");
      foreach my $type(@arr){
        $es_doc{organism}{$type} = $es_doc{cellLine}{$type};
      }
    }
    %{$converted{$key}} = %es_doc;
  }
  &insert_into_es(\%converted,"specimen");
}

sub process_organisms(){
  my %organisms = %{$_[0]};
  my %converted;
  foreach my $accession (keys %organisms){
    my $organism = $organisms{$accession};
    my $relations = fetch_json_by_url($$organism{_links}{relations}{href});
    my $childOf = fetch_json_by_url($$relations{_links}{childOf}{href});
    my $sameAs = fetch_json_by_url($$relations{_links}{sameAs}{href});

    my %es_doc = (
      #animal section of ruleset
      organism => {
        text => $$organism{characteristics}{organism}[0]{text},
        ontologyTerms => $$organism{characteristics}{organism}[0]{ontologyTerms}[0]
      },
      sex => {
        text => $$organism{characteristics}{sex}[0]{text},
        ontologyTerms => $$organism{characteristics}{sex}[0]{ontologyTerms}[0]
      },
      birthDate => {
          text => $$organism{characteristics}{birthDate}[0]{text},
          unit => $$organism{characteristics}{birthDate}[0]{unit}
      },
      breed => {
        text => $$organism{characteristics}{breed}[0]{text},
        ontologyTerms => $$organism{characteristics}{breed}[0]{ontologyTerms}[0]
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
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$organism)};

    my @healthStatus;
    foreach my $healthStatus (@{$$organism{characteristics}{healthStatus}}){
      push(@healthStatus, {text => $$healthStatus{text}, ontologyTerms => $$healthStatus{ontologyTerms}[0]});
    }
    @{$es_doc{healthStatus}} = @healthStatus;
    foreach my $samplesrelations (@{$$childOf{_embedded}{samplesrelations}}){
      push(@{$es_doc{childOf}}, $$samplesrelations{accession});
    }
    foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
      push(@{$es_doc{sameAs}}, $$sameasrelations{accession});
    }
    $organismInfoForSpecimen{$accession}{biosampleId} = $$organism{accession};
    $organismInfoForSpecimen{$accession}{organism} = {text => $$organism{characteristics}{organism}[0]{text}, ontologyTerms => $$organism{characteristics}{organism}[0]{ontologyTerms}[0]};
    $organismInfoForSpecimen{$accession}{sex} = {text => $$organism{characteristics}{sex}[0]{text}, ontologyTerms => $$organism{characteristics}{sex}[0]{ontologyTerms}[0]};
    $organismInfoForSpecimen{$accession}{breed} = {text => $$organism{characteristics}{breed}[0]{text}, ontologyTerms => $$organism{characteristics}{breed}[0]{ontologyTerms}[0]};
    @{$organismInfoForSpecimen{$accession}{healthStatus}} = @healthStatus;
    if (exists $$organism{characteristics}{strain}){
      $es_doc{breed} = {text => $$organism{characteristics}{strain}[0]{text}, ontologyTerms => $$organism{characteristics}{strain}[0]{ontologyTerms}[0]};
      $organismInfoForSpecimen{$accession}{breed} = {text => $$organism{characteristics}{strain}[0]{text}, ontologyTerms => $$organism{characteristics}{strain}[0]{ontologyTerms}[0]};
    }
    %{$converted{$accession}} = %es_doc;
  }
  &insert_into_es(\%converted,"organism");
}
#add basic information of BioSample record which is expected to exist for every single record into given hash
#the basic set is compiled from cell line
sub populateBasicBiosampleInfo(){
  my %result = %{$_[0]};
  my $biosample = $_[1];
  $result{name} = $$biosample{name};
  $result{biosampleId} = $$biosample{accession};
  $result{description} = $$biosample{description};
  $result{releaseDate} = $$biosample{releaseDate};
  $result{updateDate} = $$biosample{updateDate};
  $result{material} = {
    text => $$biosample{characteristics}{material}[0]{text},
    ontologyTerms => $$biosample{characteristics}{material}[0]{ontologyTerms}[0]
  };
  $result{project} = $$biosample{characteristics}{project}[0]{text};
  $result{availability} = $$biosample{characteristics}{availability}[0]{text};
  foreach my $organization (@{$$biosample{organization}}){
    push(@{$result{organization}}, {name => $$organization{Name}, role => $$organization{Role}, URL => $$organization{URL}});
  }
  return \%result;
}
#fetch specimen data from BioSample and populate six hashes according to their material type
sub fetch_specimens_by_project {
  my ( $project_keyword ) = @_;
  my %hash;
  my $url = "https://www.ebi.ac.uk/biosamples/api/samples/search/findByText?text=project_crt:".$project_keyword;
  my @samples = fetch_biosamples_json($url);
  foreach my $sample (@samples){
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
    if($material eq "pool of specimens"){
      $pool_specimen{$$sample{accession}} = $sample;
    }
    $hash{$material}++;
  }
#  foreach my $type(keys %hash){
#    print "There are $hash{$type} $type records\n";
#  }
}
#use BioSample API to retrieve BioSample records
sub fetch_biosamples_json{
  my ($json_url) = @_;

  my $json_text = &fetch_json_by_url($json_url);
  
  my @biosamples;
  # Store the first page 
  foreach my $item (@{$json_text->{_embedded}{samples}}){ 
    push(@biosamples, $item);
  }
  # Store each additional page
  while ($$json_text{_links}{next}{href}){  # Iterate until no more pages using HAL links
    $json_text = fetch_json_by_url($$json_text{_links}{next}{href});# Get next page
    foreach my $item (@{$json_text->{_embedded}{samples}}){
      push(@biosamples, $item);  
    }
  }
  return @biosamples;
}

sub fetch_json_by_url{
  my ($json_url) = @_;

  my $browser = WWW::Mechanize->new();
  #$browser->show_progress(1);  # Enable for WWW::Mechanize GET logging
  $browser->get( $json_url );
  my $content = $browser->content();
  my $json = new JSON;
  my $json_text = $json->decode($content);
  return $json_text;
}
#get one BioSample record with the given accession
#the returned value has the same data structure as %derivedFromOrganism 
#for development purpose: much quicker to get one record than get all records
sub fetch_single_record{
  my ($accession) = @_;
  my $url = "http://www.ebi.ac.uk/biosamples/api/samples/$accession";
  my $json_text = &fetch_json_by_url($url);
  my %hash;
  $hash{$json_text->{accession}} = $json_text;
  return %hash;
}

#delete records in ES which no longer exists in BioSample
#BE careful, this no-more-existances could be caused by lost of server
sub clean_elasticsearch{
  my ($type) = @_;
  # A scrolled search is a search that allows you to keep pulling results until there are no more matching results, much like a cursor in an SQL database.
  # Unlike paginating through results (with the from parameter in search()), scrolled searches take a snapshot of the current state of the index.
  # scroll: keeps track of which results have already been returned and so is able to return sorted results more efficiently than with deep pagination
  # scan search: disables any scoring or sorting and to return results in the most efficient way possibl
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
#get information for derivedFrom, sameAs and organism from the relations url and save in a hash 
#which has key as one of derivedFrom, sameAs and organism and value as an array of BioSample accessions
#two parameters: relationship url and level (1 for specimen from organism or 2 for all other specimen types)
sub parseRelationships(){
  my %relationships;
  my ($url,$level) = @_;
  #Pull in derived from accession from BioSamples.  #TODO This is slow, better way to do this?
  #relations have links for derivedFrom, childOf, parentOf, sameAs etc. 
 
  my $relations = fetch_json_by_url($url);#e.g. url http://www.ebi.ac.uk/biosamples/api/samplesrelations/SAMEA3540911

  my @derivedFromAccession;
  my %organismAccession;
  if($level >= 2){
    my $derivedFrom = fetch_json_by_url($$relations{_links}{derivedFrom}{href}); #Specimen from Organism e.g. url http://www.ebi.ac.uk/biosamples/api/samplesrelations/SAMEA3540911/derivedFrom
    foreach my $specimenFromOrganism(@{$$derivedFrom{_embedded}{samplesrelations}}){
      push (@derivedFromAccession,$$specimenFromOrganism{accession});
      next unless (exists $$specimenFromOrganism{_links}{derivedFrom}{href});#cell line may not have organism info
      my $organismJson = fetch_json_by_url($$specimenFromOrganism{_links}{derivedFrom}{href});
      foreach my $organism (@{$$organismJson{_embedded}{samplesrelations}}){
        $organismAccession{$$organism{accession}} = 1;
      }
    }
  }else{
    my $derivedFrom = fetch_json_by_url($$relations{_links}{derivedFrom}{href});
    push(@derivedFromAccession,$$derivedFrom{_embedded}{samplesrelations}[0]{accession});
    $organismAccession{$$derivedFrom{_embedded}{samplesrelations}[0]{accession}}=1;
  }
  if($level == 3){
    foreach my $acc(@derivedFromAccession){
      my %tmp = &fetch_single_record($acc);
      my $material = $tmp{$acc}{characteristics}{material}[0]{text};
      $organismAccession{$acc} = 1 if ($material eq "organism");
    }
  }
  @{$relationships{derivedFrom}} = @derivedFromAccession;
  @{$relationships{organism}} = keys %organismAccession;
  #Pull in sameas accession from BioSamples.  #TODO This is slow, better way to do this?
  my $sameAs = fetch_json_by_url($$relations{_links}{sameAs}{href});
  foreach my $sameasrelations (@{$$sameAs{_embedded}{samplesrelations}}){
    push(@{$relationships{sameAs}}, $$sameasrelations{accession});
  }
  return \%relationships;
}

#convert seconds into hours, minutes and seconds, for profiling purpose
sub convertSeconds(){
  my $diff = $_[0];
  my $second = $diff % 60;
  my $minute = 0;
  my $hour = 0;
  my $remaining = ($diff - $second)/60;
  $minute = $remaining % 60;
  $hour = ($remaining - $minute)/60;
  print "Elapse: $hour hour $minute minute $second second\n";
}

#return the filename extracted from the given URL. If it is not a pdf file, return null
sub getFilenameFromURL(){
    my $url = $_[0];
    my $idx = rindex ($url,".");
    my $suffix = lc(substr($url,$idx+1));
    return unless ($suffix eq "pdf");
    $idx = rindex ($url,"/");
    my $filename = substr($url,$idx+1);
    return $filename;
}
#validate multiple FAANG sample records
#if it is valid, then set field standardMet and versionLastStandardMet and
#insert (es term index) biosample entries into elasticsearch
sub insert_into_es(){
  my ($hashref,$type)=@_;
  my %converted=%{$hashref};
  my %validationResult = &validateTotalSampleRecords(\%converted,$type,\@rulesets);

  OUTER:
  foreach my $biosampleId (sort {$a cmp $b} keys %converted){
    $indexed_samples{$biosampleId} = 1;
    my %es_doc = %{$converted{$biosampleId}};
    #assign the standardMet in the order of @rulesets
    #even validations against all rulesets fail, still need to insert into ES, just leave standardMet field empty
    foreach my $ruleset(@rulesets){
      if ($validationResult{$ruleset}{detail}{$biosampleId}{status} eq "error"){
        print ERR "$biosampleId\t$validationResult{$ruleset}{detail}{$biosampleId}{type}\terror\t$validationResult{$ruleset}{detail}{$biosampleId}{message}\n";
      }else{
        $es_doc{standardMet} = $standards{$ruleset};
        #only assign version value for FAANG standard ruleset
        $es_doc{versionLastStandardMet} = $ruleset_version if ($es_doc{standardMet} eq "FAANG");
        last;
      }
    }
    #trapping error: the code can continue to run even after the die or errors, and it also captures the errors or dieing words.
    eval{
      $es->index(
        index => $es_index_name,
        type => $type,
        id => $biosampleId,
        body => \%es_doc
      );
    };
    if (my $error = $@) {
      die "error indexing sample in $es_index_name index:".$error->{text};
    }
  }
}