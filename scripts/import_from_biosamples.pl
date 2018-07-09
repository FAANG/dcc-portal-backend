#!/usr/bin/env perl
#It is strongly recommended to read this code while referring to the sample ruleset http://www.ebi.ac.uk/vg/faang/rule_sets/FAANG%20Samples
#similar to referring to corresponding xsd file while writing codes for parsing the xml file
#e.g. whether use array or hash depends on cardinality
#e.g. the description of deriveFrom and sameAs could determine how to deal with relationship

#the normal print channel is free to use as it is redirected to /dev/null in the cron jobs, only error will be reported via email
use strict;
use warnings;
use Getopt::Long;
use Carp;
use WWW::Mechanize;
use JSON -support_by_pp;
use Search::Elasticsearch;
#use List::Compare;
use Data::Dumper;
#the library file for validation of sample records
require "validate_sample_record.pl";
require "misc.pl";

my %knownColumns;
#the known column names according to the rule set https://www.ebi.ac.uk/vg/faang/rule_sets/FAANG%20Samples
#after removing these known columns, the left ones are the custom fields which need to be dealt with
my @commonColumns = ("description","Material","project","availability","same as");
my @knownOrganismColumns = ("Organism","Sex","birth date","breed","health status","birth location","birth location longitude","birth location latitude","birth weight","placental weight","pregnancy length","delivery timing","delivery ease","Child Of","pedigree","strain");#some submitter use strain instead of breed
@{$knownColumns{"organism"}} = @knownOrganismColumns;
my @knownSpecimenFromColumns = ("specimen collection date","animal age at collection","developmental stage","health status at collection","organism part","specimen collection protocol","fasted status","number of pieces","specimen volume","specimen size","specimen weight","specimen picture url","gestational age at sample collection");
@{$knownColumns{"specimen from organism"}} = @knownSpecimenFromColumns;
my @knownPoolSpecimenColumns = ("pool creation date","pool creation protocol","specimen volume","specimen size","specimen weight","specimen picture url");
@{$knownColumns{"pool of specimens"}} = @knownPoolSpecimenColumns;
my @knownCellSpecimenColumns = ("markers","cell type","purification protocol");
@{$knownColumns{"cell specimen"}} = @knownCellSpecimenColumns;
my @knownCellCultureColumns = ("culture type","cell type","cell culture protocol","culture conditions","number of passages");
@{$knownColumns{"cell culture"}} = @knownCellCultureColumns;
my @knownCellLineColumns = ("cell line","biomaterial provider","catalogue number","number of passages","date established","publication","cell type","culture conditions","culture protocol","disease","karyotype");
@{$knownColumns{"cell line"}} = @knownCellLineColumns;

#the code to test getFilenameFromURL
#my $url = "http://www.ncbi.nlm.nih.gov/pubmed/16215741";
#$url = "ftp://ftp.faang.ebi.ac.uk/ftp/protocols/samples/KU_Pool_creation_protocol_20170523.pdf";
#my $filename = &getFilenameFromURL($url);
#print "$filename\n";

##################################################################
## the section below is for development purpose by checking individual BioSample instead of reading all entries from BioSample
#my $accession = "SAMEA3540916"; #only cell line, no relationship expected
#my $accession = "SAMEA5584168"; #specimen from organism, the cell line derive from
#my $accession = "SAMEA6688918"; # the organism of 5584168
#my $accession = "SAMEA5178418"; #organism with childOf
#my $accession = "SAMEA4447317"; #organism with sameAs  

#my $accession = "SAMEA3540911"; #pool specimen
#my $accession = "SAMEA3540915"; #pool of specimen
#my $accession = "SAMEA6641668"; #cell specimen
#my $accession = "SAMEA4447551"; #cell culture
#my $accession = "SAMEA104626885";
#my $accession = "SAMEA104618854";
#test case for custom column
#my $accession = "SAMEA4447799"; #orgamism, parturition trait and lactation duration
#my $accession = "SAMEA4451615"; #organism, environmental conditions and physiological conditions
#my $accession = "SAMEA103988626"; #specimen from organism, physiological conditions
#my $accession = "SAMEA4451620"; #specimen from organism, physiological conditions which contains lactation duration information

#my $accession = "SAMEA4448136"; #organism without relationship   
#my %tmp = &fetch_single_record($accession);
#print Dumper(\%tmp);
#&process_pool_specimen(\%tmp);
#&process_organisms(\%tmp);
#&process_specimens(\%tmp);
#&process_cell_lines(\%tmp);
#&process_cell_cultures(\%tmp);
#&process_cell_specimens(\%tmp);
#exit;
##################################################################

#the parameters expected to be retrieved from the command line
#my ($project, $es_host, $es_index_name, $error_log);
my ($es_host, $es_index_name, $error_log, $legacy_flag);
$error_log = "import_biosample_error.log";
#Parse the command line options
#Example: perl import_from_biosamples.pl -project faang -es_host <elasticsearch server> -es_index_name faang
GetOptions(
#  'project=s' => \$project,
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
  'error_log=s' =>\$error_log,
  'legacy' =>\$legacy_flag
);
#croak "Need -project e.g. faang" unless ( $project);
croak "Need -es_host e.g. ves-hx-e4:9200" unless ( $es_host);
croak "Need -es_index_name e.g. faang, faang_build_1" unless ( $es_index_name);


#legacy API
#my $url = "https://www.ebi.ac.uk/biosamples/samples/search/findByText?text=".$project_keyword;
#test server
#my $url = "https://wwwdev.ebi.ac.uk/biosamples/samples?filter=attr%3Aproject%3AFAANG";
#production server
#my $url = "https://www.ebi.ac.uk/biosamples/samples?filter=attr%3Aproject%3AFAANG";
#my $url = "https://www.ebi.ac.uk/biosamples/samples?size=1000&sort=id,asc&filter=attr%3Aproject%3AFAANG";

my @legacy_animals = ("Gallus gallus","Ovis aries","Sus scrofa","Bos taurus","Capra hircus","Equus caballus");

print "The information of invalid records will be stored in $error_log\n\n";
open ERR,">$error_log";


#the curl commands are mainly called in two places
#one is here to get ruleset version
#the other is to validate all samples against rulesets in the insert_into_es()

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
#keys are specimen biosample id and values are organism biosample ids
#the purpose is to quickly determine organism id for specimen other than specimen from organism (more than one step needed)
#this only works when the order is strictly obeyed: 1) organism 2) specimen from organism 3) cell specimen 4) cell culture
#other types of specimen: cell line and pool of specimen （must be after specimen from organism as derivedFrom) should be free to place, but safe to place at the last
my %specimen_organism_relationship;
#the data structure stores information which needs to be added to specimen as organism section
my %organismInfoForSpecimen;
#keys are organism accession and values are how many specimens related to that organism
my %organismReferredBySpecimen;

print "The program starts at ".localtime."\n";
my $pastTime = time;
my $savedTime = time;

#retrieve all FAANG BioSamples from BioSample database
print "$legacy_flag\n";
if($legacy_flag){
  print "Importing legacy data\n";
  my $url = "https://www.ebi.ac.uk/biosamples/samples?size=1000&filter=attr%3AOrganism%3A";
  &fetch_records_by_species($url);
  print "There are ".(scalar keys %organism)." organisms for Legacy data\n";
  print "There are ".(scalar keys %specimen_from_organism)." specimens for Legacy data\n";
  print "There are ".(scalar keys %cell_specimen)." cell specimens for Legacy data\n";
  print "There are ".(scalar keys %cell_culture)." cell cultures for Legacy data\n";
  print "There are ".(scalar keys %cell_line)." cell lines for Legacy data\n";
  print "There are ".(scalar keys %pool_specimen)." pools of specimens for Legacy data\n";

}else{
  print "Importing FAANG data\n";
  my $url = "https://www.ebi.ac.uk/biosamples/samples?size=1000&filter=attr%3Aproject%3AFAANG";
  &fetch_records_by_project($url);
}

exit;
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

#my @accs = sort {substr($a,5) <=> substr($b,5)} keys %cell_specimen;
#$"="\n";
#print "@accs\n";
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
&convertSeconds($current - $pastTime);
$pastTime = $current;
#parse all types of specimen, at the moment the order does not matter
print "Indexing specimen from organism starts at ".localtime."\n";
&process_specimens(\%specimen_from_organism);
$current = time;
&convertSeconds($current - $pastTime);
$pastTime = $current;
#print Dumper(\%specimen_organism_relationship);

print "Indexing cell specimens starts at ".localtime."\n";
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
#my $numFromOrganisms = scalar @allOrganismList;
my @organismReferredList = keys %organismReferredBySpecimen;
#my $numFromSpecimenReferal = scalar @organismReferredList;
#print "Direct count: $numFromOrganisms\nRefer count:$numFromSpecimenReferal\n";
my %union;
foreach my $acc(@allOrganismList){
  $union{$acc}{count}++;
  push(@{$union{$acc}{source}},"organism");
}
foreach my $acc(@organismReferredList){
  $union{$acc}{count}++;
  push(@{$union{$acc}{source}},"specimen");
}
foreach my $acc(keys %union){
  print ERR "$acc only in source @{$union{$acc}{source}}\n" if ($union{$acc}{count}==1);
}

#List::Compare - Compare elements of two or more lists
#my $lc = List::Compare->new(\@allOrganismList, \@organismReferredList);
#my @organismsNotImported = $lc->get_unique;
#unless ( scalar(@organismsNotImported) < 1){
#  print Dumper(@organismsNotImported);
#  croak "Have Organisms that have not been imported \@organismsNotImported";
#}

#Delete removed samples
clean_elasticsearch('specimen');
clean_elasticsearch('organism');
$current = time;
#print "Total ";
#&convertSeconds($current - $savedTime);
print "The program ends at ".localtime."\n";


#process specimen from organism
sub process_specimens{
  my %specimen_from_organism = %{$_[0]};
  my %converted;
  foreach my $key (keys %specimen_from_organism){
    my $specimen = $specimen_from_organism{$key};

#    my %relationships = %{&parseRelationships($$specimen{_links}{relations}{href},1)};
    my %relationships = &parse_relationship($specimen);

    my $url = $$specimen{characteristics}{"specimen collection protocol"}[0]{text};
    my $filename = &getFilenameFromURL($url);
#    print "$filename\n";
    my @organisms = keys %{$relationships{derivedFrom}};
    my $organismAccession = $organisms[0];
    $specimen_organism_relationship{$key} = $organismAccession;
    my %es_doc = (
      derivedFrom => $organismAccession,
      specimenFromOrganism => {
        specimenCollectionDate => {
          text => $$specimen{characteristics}{"specimen collection date"}[0]{text},
          unit => $$specimen{characteristics}{"specimen collection date"}[0]{unit}
        },
        animalAgeAtCollection => {
          text => $$specimen{characteristics}{"animal age at collection"}[0]{text},
          unit => $$specimen{characteristics}{"animal age at collection"}[0]{unit}
        },
        developmentalStage => {
          text => $$specimen{characteristics}{"developmental stage"}[0]{text},
          ontologyTerms => $$specimen{characteristics}{"developmental stage"}[0]{ontologyTerms}[0]
        },
        organismPart => {
          text => $$specimen{characteristics}{"organism part"}[0]{text},
          ontologyTerms => $$specimen{characteristics}{"organism part"}[0]{ontologyTerms}[0]
        },
#        specimenCollectionProtocol => $$specimen{characteristics}{specimenCollectionProtocol}[0]{text},
        specimenCollectionProtocol => {
          url => $url,
          filename => $filename
        },
        fastedStatus => $$specimen{characteristics}{"fasted status"}[0]{text},
        numberOfPieces => {
          text => $$specimen{characteristics}{"number of pieces"}[0]{text},
          unit => $$specimen{characteristics}{"number of pieces"}[0]{unit}
        },
        specimenVolume => {
          text => $$specimen{characteristics}{"specimen volume"}[0]{text},
          unit => $$specimen{characteristics}{"specimen volume"}[0]{unit}
        },
        specimenSize => {
          text => $$specimen{characteristics}{"specimen size"}[0]{text},
          unit => $$specimen{characteristics}{"specimen size"}[0]{unit}
        },
        specimenWeight => {
          text => $$specimen{characteristics}{"specimen weight"}[0]{text},
          unit => $$specimen{characteristics}{"specimen weight"}[0]{unit}
        },
        gestationalAgeAtSampleCollection => {
          text => $$specimen{characteristics}{"gestational age at sample collection"}[0]{text},
          unit => $$specimen{characteristics}{"gestational age at sample collection"}[0]{unit}
        }
      }
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$specimen)};
    %es_doc = %{&extractCustomField(\%es_doc,$specimen,"specimen from organism")};

    $es_doc{cellType} = {
      text => $$specimen{characteristics}{"organism part"}[0]{text}, 
      ontologyTerms => $$specimen{characteristics}{"organism part"}[0]{ontologyTerms}[0]
    };

    foreach my $specimenPictureUrl (@{$$specimen{characteristics}{"specimen picture url"}}){
      push(@{$es_doc{specimenFromOrganism}{specimenPictureUrl}}, $$specimenPictureUrl{text});
    }
    foreach my $healthStatusAtCollection (@{$$specimen{characteristics}{"health status at collection"}}){
      push(@{$es_doc{specimenFromOrganism}{healthStatusAtCollection}}, {text => $$healthStatusAtCollection{text}, ontologyTerms => $$healthStatusAtCollection{ontologyTerms}[0]});
    }
    @{$es_doc{sameAs}} = keys %{$relationships{sameAs}} if (exists $relationships{sameAs});
#    my @organisms = keys %{$relationships{organism}} 

    $es_doc{organism}=$organismInfoForSpecimen{$organismAccession};
    $organismReferredBySpecimen{$organismAccession}++;

    #the validation service can only be applied on FAANG data, not BioSample data
    #therefore it needs to be converted first using the codes above, and save into %converted
    #it should be more efficient to validate multiple records than one record at a time
    %{$converted{$key}} = %es_doc;
#    print Dumper(\%es_doc);
#    exit;
  }
  #only insert validated entries
  &insert_into_es(\%converted,"specimen");
}

sub process_pool_specimen{
  my %pool_specimen = %{$_[0]};
  my %converted;
  foreach my $accession(keys %pool_specimen){
    my $specimen = $pool_specimen{$accession};#e.g. accession = SAMEA3540911
    my %relationships = &parse_relationship($specimen);

    my $url = $$specimen{characteristics}{"pool creation protocol"}[0]{text};
    my $filename = &getFilenameFromURL($url);

    my %es_doc = (
#      sameAs => ,     #according to ruleset, it should be single value entry, i.e. use a hash. However for all other types, an array is used, to make it consistent, use array here as well
      poolOfSpecimens => {
        poolCreationDate => {
          text => $$specimen{characteristics}{"pool creation date"}[0]{text},
          unit => $$specimen{characteristics}{"pool creation date"}[0]{unit}
        },
        poolCreationProtocol => {
          url => $url,
          filename => $filename
        },
        specimenVolume => {     #no example in the current FAANG collection for pool of specimens, pure guess, expect to change later when data becomes available
          text => $$specimen{characteristics}{"specimen volume"}[0]{text},
          unit => $$specimen{characteristics}{"specimen volume"}[0]{unit}
        },
        specimenSize => {       #no example in the current FAANG collection for pool of specimens, pure guess, expect to change later when data becomes available
          text => $$specimen{characteristics}{"specimen size"}[0]{text},
          unit => $$specimen{characteristics}{"specimen size"}[0]{unit}
        },
        specimenWeight => {     #no example in the current FAANG collection for pool of specimens, pure guess, expect to change later when data becomes available
          text => $$specimen{characteristics}{"specimen weight"}[0]{text},
          unit => $$specimen{characteristics}{"specimen weight"}[0]{unit}
        }
      }
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$specimen)};
    %es_doc = %{&extractCustomField(\%es_doc,$specimen,"pool of specimens")};

    $es_doc{cellType} = {text => "Not Applicable"};
    foreach my $spu (@{$$specimen{characteristics}{"specimen picture url"}}){ #no example in the current FAANG collection for pool of specimens, pure guess, expect to change later when data becomes available
      push (@{$es_doc{poolOfSpecimens}{specimenPictureUrl}},$$spu{text});
    }

    #at the moment, organism information is not stored as it can be retrieved from each individual derivedFrom specimen
    #if in future it is needed, e.g. to reduce calculation time for frontend, the logic should be placed in the foreach loop below
    my %tmp;
    if (exists $relationships{derivedFrom}){
      my @derivedFrom = keys %{$relationships{derivedFrom}};#must be specimen from organism
#      print "pool specimen $accession derive from <@derivedFrom>\n";
      @{$es_doc{derivedFrom}} = @derivedFrom;
      foreach my $derivedFrom (@derivedFrom){
        my $organismAccession = "";
        if (exists $specimen_organism_relationship{$derivedFrom}){
          $organismAccession = $specimen_organism_relationship{$derivedFrom};
          $organismReferredBySpecimen{$organismAccession}++;
          unless (exists $organismInfoForSpecimen{$organismAccession}){
            print ERR "No organism information found for organism <$organismAccession> which is for $accession\n";
            next;
          }
#          print "$organismAccession\n";
#          print Dumper($organismInfoForSpecimen{$organismAccession});
          $tmp{organism}{$organismInfoForSpecimen{$organismAccession}{organism}{text}} = $organismInfoForSpecimen{$organismAccession}{organism}{ontologyTerms};
          $tmp{sex}{$organismInfoForSpecimen{$organismAccession}{sex}{text}} = $organismInfoForSpecimen{$organismAccession}{sex}{ontologyTerms};
          $tmp{breed}{$organismInfoForSpecimen{$organismAccession}{breed}{text}} = $organismInfoForSpecimen{$organismAccession}{breed}{ontologyTerms};
        }else{
          print ERR "No organism found for specimen $derivedFrom\n";
        }
      } 
    }
    @{$es_doc{sameAs}} = @{$relationships{sameAs}} if (exists $relationships{sameAs});
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
#    my %relationships = %{&parseRelationships($$specimen{_links}{relations}{href},2)};
    my %relationships = &parse_relationship($specimen);
    
    my $url = $$specimen{characteristics}{"purification protocol"}[0]{text};
    my $filename = &getFilenameFromURL($url);

    #cell specimen derive from specimen from organism
    #therefore two steps needed to get organism: specimen from organism(sfo) first, then organism
    my @derivedFrom = keys %{$relationships{derivedFrom}};
    my $specimenFromOrganismAccession = $derivedFrom[0];
#    my %sfo_data;
#    if (exists $specimen_from_organism{$specimenFromOrganismAccession}){
#      %sfo_data = %{$specimen_from_organism{$specimenFromOrganismAccession}};
#    }else{
#      my %tmp = &fetch_single_record($specimenFromOrganismAccession);
#      %sfo_data = %{$tmp{$specimenFromOrganismAccession}};
#    }
#    my %relOrganism = &parse_relationship(\%sfo_data);
#    my @organisms = keys %{$relOrganism{derivedFrom}};
    my $organismAccession = "";
    if (exists $specimen_organism_relationship{$specimenFromOrganismAccession}){
      $organismAccession = $specimen_organism_relationship{$specimenFromOrganismAccession};
    }
    $specimen_organism_relationship{$key} = $organismAccession;
    my %es_doc = (
      derivedFrom => $specimenFromOrganismAccession,
      cellSpecimen => {
        markers => $$specimen{characteristics}{markers}[0]{text},
        purificationProtocol => {
          url => $url,
          filename => $filename
        }        
      }
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$specimen)};
    %es_doc = %{&extractCustomField(\%es_doc,$specimen,"cell specimen")};

    $es_doc{cellType} = {
      text => $$specimen{characteristics}{"cell type"}[0]{text}, 
      ontologyTerms => $$specimen{characteristics}{"cell type"}[0]{ontologyTerms}[0]
    };

    foreach my $cellType (@{$$specimen{characteristics}{"cell type"}}){
      push(@{$es_doc{cellSpecimen}{cellType}}, $cellType);
    }
    @{$es_doc{sameAs}} = @{$relationships{sameAs}} if (exists $relationships{sameAs});

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
    my %relationships = &parse_relationship($specimen);

    my $url = $$specimen{characteristics}{"cell culture protocol"}[0]{text};
    my $filename = &getFilenameFromURL($url);

    #cell culture derive from specimen from organism
    #therefore two steps needed to get organism: specimen from organism(sfo) first, then organism
    my @derivedFrom = keys %{$relationships{derivedFrom}};
    my $derivedFromAccession = $derivedFrom[0];
    my $organismAccession = "";
    if (exists $specimen_organism_relationship{$derivedFromAccession}){
      $organismAccession = $specimen_organism_relationship{$derivedFromAccession};
    }
    $specimen_organism_relationship{$key} = $organismAccession;

    my %es_doc = (
      derivedFrom => $derivedFromAccession,
      cellCulture => {
        cultureType => {
          text => $$specimen{characteristics}{"culture type"}[0]{text},
          ontologyTerms => $$specimen{characteristics}{"culture type"}[0]{ontologyTerms}[0],
        },
        cellType => {
          text => $$specimen{characteristics}{"cell type"}[0]{text},
          ontologyTerms => $$specimen{characteristics}{"cell type"}[0]{ontologyTerms}[0],
        },
        cellCultureProtocol => {
          url => $url,
          filename => $filename
        },
        cultureConditions => $$specimen{characteristics}{"culture conditions"}[0]{text},
        numberOfPassages => $$specimen{characteristics}{"number of passages"}[0]{text}
      }
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$specimen)};
    %es_doc = %{&extractCustomField(\%es_doc,$specimen,"cell culture")};

    $es_doc{cellType} = {
      text => $$specimen{characteristics}{"cell type"}[0]{text}, 
      ontologyTerms => $$specimen{characteristics}{"cell type"}[0]{ontologyTerms}[0]
    };

    @{$es_doc{sameAs}} = @{$relationships{sameAs}} if (exists $relationships{sameAs});

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
    my %relationships = &parse_relationship($specimen);
#    my %relationships = %{&parseRelationships($$specimen{_links}{relations}{href},3)};

    my $url;
    my $filename ;
    if (exists $$specimen{characteristics}{"culture protocol"}[0]{text}){
      $url = $$specimen{characteristics}{"culture protocol"}[0]{text};
      $filename = &getFilenameFromURL($url);
    }
    my %es_doc = (
      cellLine => {
        organism => {
          text => $$specimen{characteristics}{Organism}[0]{text},
          ontologyTerms => $$specimen{characteristics}{Organism}[0]{ontologyTerms}[0]
        },
        sex => {
          text => $$specimen{characteristics}{Sex}[0]{text},
          ontologyTerms => $$specimen{characteristics}{Sex}[0]{ontologyTerms}[0]
        },
        cellLine => $$specimen{characteristics}{"cell line"}[0]{text},
        biomaterialProvider => $$specimen{characteristics}{"biomaterial provider"}[0]{text},
        catalogueNumber => $$specimen{characteristics}{"catalogue number"}[0]{text},
        numberOfPassages => $$specimen{characteristics}{"number of passages"}[0]{text},
        dateEstablished => { 
          text => $$specimen{characteristics}{"date established"}[0]{text},
          unit => $$specimen{characteristics}{"date established"}[0]{unit}
        },
        publication => $$specimen{characteristics}{publication}[0]{text},
        breed => {
          text => $$specimen{characteristics}{breed}[0]{text},
          ontologyTerms => $$specimen{characteristics}{breed}[0]{ontologyTerms}[0]
        },
        cellType => {
          text => $$specimen{characteristics}{"cell type"}[0]{text},
          ontologyTerms => $$specimen{characteristics}{"cell type"}[0]{ontologyTerms}[0]
        },
        cultureConditions => $$specimen{characteristics}{"culture conditions"}[0]{text},
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
    %es_doc = %{&extractCustomField(\%es_doc,$specimen,"cell line")};

    $es_doc{cellType} = {
      text => $$specimen{characteristics}{"cell type"}[0]{text}, 
      ontologyTerms => $$specimen{characteristics}{"cell type"}[0]{ontologyTerms}[0]
    };

    $es_doc{derivedFrom} = $relationships{derivedFrom}[0] if (exists $relationships{derivedFrom});
    @{$es_doc{sameAs}} = @{$relationships{sameAs}} if (exists $relationships{sameAs});

     #print Dumper(\%relationships);exit;
    #at the time of development there is no data in BioSample to have derived from value so this part is commented out
    #need to get some attention when the data becomes available
#    if (exists $relationships{organism} && (scalar @{$relationships{organism}})>0){ #derive from an animal, use animal as its organism
#      my $organismAccession = $relationships{organism}[0];
#      $es_doc{organism}=$organismInfoForSpecimen{$organismAccession};
#      $organismReferredBySpecimen{$organismAccession}++;
#    }else{ #derive from specimen, use the cellLine organism info
      my @arr = ("organism","sex","breed");
      foreach my $type(@arr){
        $es_doc{organism}{$type} = $es_doc{cellLine}{$type};
      }
#    }
    %{$converted{$key}} = %es_doc;
  }
  &insert_into_es(\%converted,"specimen");
}

sub process_organisms(){
#  print Dumper($_[0]);
  my %organisms = %{$_[0]};
#  my $count = scalar keys %organisms;
#  print "There are $count organisms\n";
  my %converted;
  foreach my $accession (keys %organisms){
    my $organism = $organisms{$accession};
#    print "$accession\n";
#    print Dumper($organism);
    my %es_doc = (
      #animal section of ruleset
      organism => {
        text => $$organism{characteristics}{Organism}[0]{text},
        ontologyTerms => $$organism{characteristics}{Organism}[0]{ontologyTerms}[0]
      },
      sex => {
        text => $$organism{characteristics}{Sex}[0]{text},
        ontologyTerms => $$organism{characteristics}{Sex}[0]{ontologyTerms}[0]
      },
      birthDate => {
          text => $$organism{characteristics}{"birth date"}[0]{text},
          unit => $$organism{characteristics}{"birth date"}[0]{unit}
      },
      breed => {
        text => $$organism{characteristics}{breed}[0]{text},
        ontologyTerms => $$organism{characteristics}{breed}[0]{ontologyTerms}[0]
      },
      birthLocation => $$organism{characteristics}{"birth location"}[0]{text},
      birthLocationLongitude => {
          text => $$organism{characteristics}{"birth location longitude"}[0]{text},
          unit => $$organism{characteristics}{"birth location longitude"}[0]{unit}
      },
      birthLocationLatitude => {
          text => $$organism{characteristics}{"birth location latitude"}[0]{text},
          unit => $$organism{characteristics}{"birth location latitude"}[0]{unit}
      },
      birthWeight => {
          text => $$organism{characteristics}{"birth weight"}[0]{text},
          unit => $$organism{characteristics}{"birth weight"}[0]{unit}
      },
      placentalWeight => {
          text => $$organism{characteristics}{"placental weight"}[0]{text},
          unit => $$organism{characteristics}{"placental weight"}[0]{unit}
      },
      pregnancyLength => {
          text => $$organism{characteristics}{"pregnancy length"}[0]{text},
          unit => $$organism{characteristics}{"pregnancy length"}[0]{unit}
      },
      deliveryTiming => $$organism{characteristics}{"delivery timing"}[0]{text},
      deliveryEase => $$organism{characteristics}{"delivery ease"}[0]{text},
      pedigree => $$organism{characteristics}{pedigree}[0]{text}
    );
    %es_doc = %{&populateBasicBiosampleInfo(\%es_doc,$organism)};

    %es_doc = %{&extractCustomField(\%es_doc,$organism,"organism")};

    my @healthStatus;
    foreach my $healthStatus (@{$$organism{characteristics}{"health status"}}){
      push(@healthStatus, {
                            text => $$healthStatus{text}, ontologyTerms => $$healthStatus{ontologyTerms}[0]
                          });
    }
    @{$es_doc{healthStatus}} = @healthStatus;

    my %relationships = &parse_relationship($organism);
    @{$es_doc{childOf}} = keys %{$relationships{childOf}} if (exists $relationships{childOf});
    @{$es_doc{sameAs}} = keys %{$relationships{sameAs}} if (exists $relationships{sameAs});

    $organismInfoForSpecimen{$accession}{biosampleId} = $$organism{accession};
    $organismInfoForSpecimen{$accession}{organism} = {
      text => $$organism{characteristics}{Organism}[0]{text}, 
      ontologyTerms => $$organism{characteristics}{Organism}[0]{ontologyTerms}[0]
    };
    $organismInfoForSpecimen{$accession}{sex} = {
      text => $$organism{characteristics}{Sex}[0]{text}, 
      ontologyTerms => $$organism{characteristics}{Sex}[0]{ontologyTerms}[0]
    };
    $organismInfoForSpecimen{$accession}{breed} = {
      text => $$organism{characteristics}{breed}[0]{text}, 
      ontologyTerms => $$organism{characteristics}{breed}[0]{ontologyTerms}[0]
    };
    @{$organismInfoForSpecimen{$accession}{healthStatus}} = @healthStatus;
    if (exists $$organism{characteristics}{strain}){
      $es_doc{breed} = {text => $$organism{characteristics}{strain}[0]{text}, ontologyTerms => $$organism{characteristics}{strain}[0]{ontologyTerms}[0]};
      $organismInfoForSpecimen{$accession}{breed} = {text => $$organism{characteristics}{strain}[0]{text}, ontologyTerms => $$organism{characteristics}{strain}[0]{ontologyTerms}[0]};
    }
    %{$converted{$accession}} = %es_doc;
#    print Dumper(\%es_doc);
#    exit;
  }
  &insert_into_es(\%converted,"organism");
}
#add basic information of BioSample record which is expected to exist for every single record into given hash
#the basic set is compiled from cell line
sub populateBasicBiosampleInfo(){
  my %result = %{$_[0]};
  my $biosample = $_[1];
  $result{name} = $$biosample{name};
  my $acc = $$biosample{accession};
  $result{biosampleId} = $acc;
  $result{"id_number"} = substr($acc,5);
#  print "$acc\n$result{id_number}\n";
#  exit;
  $result{description} = $$biosample{characteristics}{description}[0]{text};#V4.0 change
#  $result{releaseDate} = $$biosample{releaseDate};
#  $result{updateDate} = $$biosample{updateDate};
#  print "no release date for $$biosample{accession}\n" unless (defined $$biosample{release});
#  print "no update date for $$biosample{accession}\n" unless (defined $$biosample{update});

  $result{releaseDate} = &parseDate($$biosample{release},$$biosample{accession});
  $result{updateDate} = &parseDate($$biosample{update},$$biosample{accession});
  $result{material} = {
    text => $$biosample{characteristics}{Material}[0]{text},
    ontologyTerms => $$biosample{characteristics}{Material}[0]{ontologyTerms}[0]
  };
  $result{project} = $$biosample{characteristics}{project}[0]{text};
  $result{availability} = $$biosample{characteristics}{availability}[0]{text};
  foreach my $organization (@{$$biosample{organization}}){
    unless (exists $$organization{Name} || exists $$organization{Role} || exists $$organization{URL}){
      print ERR "no organization at all: $$biosample{accession}\n"; 
#      print Dumper($organization);
    }
    my %tmp;
    $tmp{name} = $$organization{Name};
    $tmp{role} = &trim($$organization{Role});
    $tmp{URL} = $$organization{URL};
    push(@{$result{organization}}, \%tmp);
#      push(@{$result{organization}}, {
#                                    name => $$organization{Name}, 
#                                    role => &trim($$organization{Role}), 
#                                    URL => $$organization{URL}
#                                    });
  }
  return \%result;
}
#add basic information of BioSample record which is expected to exist for every single record into given hash
#the basic set is compiled from cell line
sub extractCustomField(){
  my %result = %{$_[0]};
  my $biosample = $_[1];
  my $materialType = $_[2];
  my %characteristics = %{$$biosample{characteristics}};
  croak "Unrecognized material type" unless (exists $knownColumns{$materialType});
  my @knownColumns = @{$knownColumns{$materialType}};

  foreach my $knownColumn (@commonColumns, @knownColumns){
    delete $characteristics{$knownColumn} if (exists $characteristics{$knownColumn});
  }

  my @customs;
  foreach my $name (keys %characteristics){
    my $type = ref($characteristics{$name});
    my $toParse;
    if ($type eq "ARRAY"){
      $toParse = $characteristics{$name}[0];
    }else{
      $toParse = $characteristics{$name};
    }
    $type = ref($toParse);
    my %tmp;
    $tmp{name} = $name;
    if ($type eq "HASH"){
      if (exists $$toParse{text}){
        $tmp{value} = $$toParse{text};
      }elsif (exists $$toParse{value}){
        $tmp{value} = $$toParse{value};
      }
      $tmp{unit} = $$toParse{unit} if (exists $$toParse{unit});
      $tmp{ontologyTerms} = $$toParse{ontologyTerms}[0] if (exists $$toParse{ontologyTerms});
    }elsif($type eq "SCALAR"){
      $tmp{value} = $toParse;
    }
    push (@customs,\%tmp);
  }
  @{$result{customField}}=@customs;
#  print Dumper(\%result);exit;
  return \%result;
}
#fetch specimen data from BioSample and populate six hashes according to their material type
sub fetch_records_by_project {
  my $url = $_[0];
  my %hash;
  my @samples = &fetch_biosamples_json($url);
#  my @samples = &fetch_biosamples_json_by_page($url);

  foreach my $sample (@samples){
    my $isFaangLabelled = &check_is_faang($sample);
    next if ($isFaangLabelled == 0);
    my $material = $$sample{characteristics}{Material}[0]{text};

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
  my $total = 0;
  foreach my $type(keys %hash){
    $total += $hash{$type};
    print "There are $hash{$type} $type records\n";
  }
  print "The sum is $total\n";
}

sub fetch_records_by_species {
  my $url = $_[0];
  foreach my $species(@legacy_animals){
    my %hash;
    my $finishedUrl = $url.$species;
    $finishedUrl =~s/ /+/;
    print "$finishedUrl\n";
    my @samples = &fetch_biosamples_json($finishedUrl);
    my $numSamples = scalar @samples;
    print "there are $numSamples records for $species\n";
    my $countFaang = 0;
    foreach my $sample (@samples){
      my $isFaangLabelled = &check_is_faang($sample);
      if ($isFaangLabelled == 1){#if is FAANG, should be dealt with without -legacy option
        $countFaang++;
#        print "$$sample{accession} is a FAANG record\n";
        next;
      }
#      print "Checking material information for non-FAANG record $$sample{accession}\n";
      next unless (exists $$sample{characteristics});
      next unless (exists $$sample{characteristics}{Material});
      my $material = $$sample{characteristics}{Material}[0]{text};
      next unless (defined($material));
      if($material eq "organism"){
        $organism{$$sample{accession}} = $sample;
      }elsif($material eq "specimen from organism"){
        $specimen_from_organism{$$sample{accession}} = $sample;
      }elsif($material eq "cell specimen"){
        $cell_specimen{$$sample{accession}} = $sample;
      }elsif($material eq "cell culture"){
        $cell_culture{$$sample{accession}} = $sample;
      }elsif($material eq "cell line"){
        $cell_line{$$sample{accession}} = $sample;
      }elsif($material eq "pool of specimens"){
        $pool_specimen{$$sample{accession}} = $sample;
      }
      $hash{$material}++;
    }
    print "$countFaang FAANG records skipped here\n";
  }
}
#fetch data from BioSample and populate six hashes according to their material type

#use BioSample API to retrieve BioSample records
sub fetch_biosamples_json_by_page{
  my ($json_url) = @_;

  my $json_text = &fetch_json_by_url($json_url);
  my @biosamples;
  my $pageNum = $json_text->{page}{totalPages};
  # Store the first page 
  foreach my $item (@{$json_text->{_embedded}{samples}}){
    push(@biosamples, $item);
  }
  # Store each additional page
  for (my $i = 1;$i<$pageNum;$i++){
    my $pageUrl = $json_url."&page=$i";
    print "$pageUrl\n" if (($i%10)==0);
    $json_text = &fetch_json_by_url($pageUrl);# Get next page
    foreach my $item (@{$json_text->{_embedded}{samples}}){
      push(@biosamples, $item);  
    }
  }
  return @biosamples;
}
#use BioSample API to retrieve BioSample records
sub fetch_biosamples_json{
  my ($json_url) = @_;
  my @biosamples;
  while ($json_url && length($json_url)>0){
    my $json_text = &fetch_json_by_url($json_url);
    #print Dumper($$json_text{page}); #contains total number, page size and page count
    foreach my $item (@{$json_text->{_embedded}{samples}}){
      push(@biosamples, $item);
    }
    $json_url = $$json_text{_links}{next}{href};
  }
  return @biosamples;
}
#move to misc.pl
#sub fetch_json_by_url{
#  my ($json_url) = @_;

#  my $browser = WWW::Mechanize->new();
  #$browser->show_progress(1);  # Enable for WWW::Mechanize GET logging
#  $browser->get( $json_url );
#  my $content = $browser->content();
#  my $json = new JSON;
#  my $json_text = $json->decode($content);
#  return $json_text;
#}
#get one BioSample record with the given accession
#the returned value has the same data structure as %derivedFromOrganism 
#for development purpose: much quicker to get one record than get all records
sub fetch_single_record{
  my ($accession) = @_;
#  my $url = "http://www.ebi.ac.uk/biosamples/api/samples/$accession"; #old API
  my $url = "http://www.ebi.ac.uk/biosamples/samples/$accession";
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

sub check_is_faang(){
  my $sample = $_[0];
  if (exists $$sample{characteristics} && exists $$sample{characteristics}{project}){
    my @tmp = @{$$sample{characteristics}{project}};
    foreach my $tmp(@tmp){
      if (exists $$tmp{text} && lc($$tmp{text}) eq "faang"){
        return 1;
      }
    }
  }
  return 0;
}

sub parse_relationship(){
  my %result;
#  print Dumper($_[0]);
  unless (exists $_[0]{relationships}){
    print ERR "No relationships for $_[0]{accession}\n";
#    return %result;
  }
  my @relations = @{$_[0]{relationships}};
  my $accession = $_[0]{accession};
  foreach my $ref(@relations){
#      push(@{$result{$$ref{type}}},$$ref{target}) unless ($$ref{target} eq $accession);
#      push(@{$result{toLowerCamelCase($$ref{type})}},$$ref{target}) unless ($$ref{target} eq $accession);
      $result{$$ref{type}}{$$ref{target}}++ unless ($$ref{target} eq $accession);
      $result{toLowerCamelCase($$ref{type})}{$$ref{target}}++ unless ($$ref{target} eq $accession);
  }
  return %result;
}

sub parseDate(){
  my $isoDate = $_[0];
  print ERR "no date value for $_[1]\n" unless (defined $isoDate);
  if ($isoDate=~/(\d+-\d+-\d+)T/){
    return $1;
  }
  return $isoDate;
}