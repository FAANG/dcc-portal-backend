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

require "validate_experiment_record.pl";
require "misc.pl";

my %categories = &initializeCategories();
my @categories = keys %categories;

my %organisms;
my %specimen_from_organisms;
my %checkedMaterial;

my %species=(
  "9031"=>"Gallus gallus",
  "9913"=>"Bos taurus",
  "9823"=>"Sus scrofa",
  "9940"=>"Ovis aries",
  "9796"=>"Equus caballus",
  "9925"=>"Capra hircus"
);
my @species = keys %species;
#&investigateFieldContent();
#&predict();
#exit;


my $es_host = 'ves-hx-e4:9200';
my $es_index_name = 'faang';
my $error_log = "import_ena_legacy_error.log";

GetOptions(
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
  'error_log=s' =>\$error_log
);

croak "Need -es_host" unless ($es_host);
my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct'); #client option to make it compatiable with elasticsearch 1.x APIs

&importBioSample("SAMEA3712494");
exit;
my %existingBiosamples;
&getExistingIDs("specimen");
&getExistingIDs("organism");


my %existingDatasets = &getFAANGstandardDatasets();

my %assayTypesToBeImported = (
  "ATAC-seq" => "ATAC-seq",
  "BS-Seq" => "methylation profiling by high throughput sequencing",
  "Hi-C" => "Hi-C"
);
my %experimentTargets = (
  "ATAC-seq" => "open_chromatin_region",
  "BS-Seq" => "DNA methylation",
  "Hi-C" => "chromatin"
);
my $fieldList = &getFieldsList();
my $speciesList = join(",",@species);

my %todo;
foreach my $term(@categories){
  my $category = $categories{$term};
  next unless (exists $assayTypesToBeImported{$category});
#  print "$term: assay type $assay_type  target $experiment_target\n";
#  foreach my $species(@species){
  my $url="https://www.ebi.ac.uk/ena/portal/api/search/?result=read_run&format=JSON&limit=0&query=library_strategy%3D%22$term%22%20AND%20tax_eq($speciesList)&fields=$fieldList";
#  print "$url\n";
  my $json_text = &getAPIresult($url);
  foreach my $record (@$json_text){
#    my $species = $$record{tax_id};
    my $study_accession = $$record{study_accession};
    next if (exists $existingDatasets{$study_accession});#already exists in ES and meet FAANG standard
    next if ($$record{project_name} eq "FAANG");#should be dealt with by sibling script
    push (@{$todo{$category}},$record);
  }
#  }
}
#print "Working on $es_index_name at $es_host\n";

#used for deleting no longer existant ES records, e.g. record with old id system
my %indexed_files;

my @data_sources = qw/fastq sra cram_index/;
my @data_types = qw/ftp galaxy aspera/;
my %datasets; 
my %experiments;
my %files;

foreach my $category(keys %todo){
  my $assay_type = $assayTypesToBeImported{$category};
  my $experiment_target = $experimentTargets{$category};
#  print "$category: assay type $assay_type  target $experiment_target\n";
#  my $len  = scalar @{$todo{$category}};
#  print "$len\n";
  foreach my $record(@{$todo{$category}}){
    my $file_type = "";
    my $source_type = "";
    OUTER:
    foreach my $data_source(@data_sources){
      foreach my $type(@data_types){
        if (exists $record->{"${data_source}_$type"} && $record->{"${data_source}_$type"} ne ""){
          $file_type = $type;
          $source_type = $data_source;
          last OUTER;
        }
      }
    }
    next if (length $file_type == 0);

    my $archive;
    if($source_type eq "fastq"){
      $archive = "ENA";
    }elsif ($source_type eq "cram_index"){
      $archive = "CRAM";
    }else{
      $archive = "SRA";
    } 
    #each file per record is for the purpose of easy sorting via ES
    my @files = split(";",$$record{"${source_type}_${file_type}"});
    my @types = split(";",$$record{submitted_format});
    my @sizes = split(";",$$record{"${source_type}_bytes"});
    my @checksums = split(";",$$record{"${source_type}_md5"});#for ENA, it is fixed to MD5 as the checksum method

    for (my $i=0;$i<scalar @files;$i++){
      my $specimen_biosample_id = $$record{sample_accession};
      #check whether exists in ES, if not import it assuming being specimen from organism
      &importBioSample($specimen_biosample_id) unless (exists $existingBiosamples{$specimen_biosample_id});
    
      my $file = $files[$i]; #full path
      my $idx = rindex ($file,"/");
      my $fullname = substr($file,$idx+1);
      $idx = index ($fullname,".");
      my $filename = substr($fullname,0,$idx);

      my %es_file = (
        specimen => $specimen_biosample_id,
        species => {
          text => $species{$$record{tax_id}},
          ontologyTerms => "http://purl.obolibrary.org/obo/NCBITaxon_$$record{tax_id}"
        },
#        species => $biosample_ids{$$record{sample_accession}}{organism}{organism},
        url => $file,
        name => $fullname,
        type => $types[$i],
        size => $sizes[$i],
        readableSize => &convertReadable($sizes[$i]),
        checksumMethod => "md5",
        checksum => $checksums[$i],
        archive => $archive,
        baseCount => $$record{base_count},
        readCount => $$record{read_count},
        releaseDate => $$record{first_public},
        updateDate => $$record{last_updated},
        submission => $$record{submission_accession},
        experiment => {
          accession => $$record{experiment_accession},
          assayType => $assay_type,
          target => $experiment_target
        },
        run => {
          accession => $$record{run_accession},
          alias => $$record{run_alias},
          platform => $$record{instrument_platform},
          instrument => $$record{instrument_model}
        },
        study => {
          accession => $$record{study_accession},
          alias => $$record{study_alias},
          title => $$record{study_title},
          type => $category,
          secondaryAccession => $$record{secondary_study_accession}
        }
      );
      %{$files{$filename}} = %es_file;
      my $exp_id = $$record{experiment_accession};
      unless (exists $experiments{$exp_id}){
        my %exp_es = (
          accession => $exp_id,
          assayType => $assay_type,
          experimentTarget => $experiment_target
        );
        %{$experiments{$exp_id}} = %exp_es;
      }

      my $dataset_id = $$record{study_accession};
      my %es_dataset;
      if (exists $datasets{$dataset_id}){
        %es_dataset = %{$datasets{$dataset_id}};
      }else{
        #the basic information of dataset which should be same across all files linking to the same dataset
        $es_dataset{accession}=$dataset_id;
        $es_dataset{alias}=$$record{study_alias};
        $es_dataset{title}=$$record{study_title};
        $es_dataset{type}=$category;
        $es_dataset{secondaryAccession}=$$record{secondary_study_accession};
      }

      $datasets{tmp}{$dataset_id}{specimen}{$specimen_biosample_id}=1;
      $datasets{tmp}{$dataset_id}{instrument}{$$record{instrument_model}} = 1;
      $datasets{tmp}{$dataset_id}{archive}{$archive} = 1;
      #related file
      my %tmp_file = (
        url => $file,
        name => $fullname,
        fileId => $filename,
        experiment => $$record{experiment_accession},
        type => $types[$i],
        size => $sizes[$i],
        readableSize => &convertReadable($sizes[$i]),
        archive => $archive,
        baseCount => $$record{base_count},
        readCount => $$record{read_count}
      );
      %{$datasets{tmp}{$dataset_id}{file}{$fullname}} = %tmp_file;
      #experiment
      my %tmp_exp = (
        accession => $$record{experiment_accession},
        assayType => $assay_type,
        target => $experiment_target
      );
      %{$datasets{tmp}{$dataset_id}{experiment}{$$record{experiment_accession}}} = %tmp_exp;

      %{$datasets{$dataset_id}} = %es_dataset;
    }#end for foreach (@files)
  }
}

my @dataset_ids = sort keys %datasets;
for (my $i=0;$i<scalar @dataset_ids;$i++){
  my $dataset_id = $dataset_ids[$i];
  next if ($dataset_id eq "tmp");
  my $num_exps = 0;
  if (exists $datasets{tmp}{$dataset_id}{experiment}){
    my %dataset_exps = %{$datasets{tmp}{$dataset_id}{experiment}};
    $num_exps = scalar keys %dataset_exps;
  }
  my $index = $i+1;
  print "$index $dataset_id has $num_exps experiments to be processed\n";
}

print "\nThere are ".((scalar keys %datasets)-1)." datasets to be processed\n"; #%dataset contains one artificial value set with the key as 'tmp'

print "The information of invalid records will be stored in $error_log\n\n";
open ERR,">$error_log";

#no need to validate against FAANG standard as we know information missing
my @rulesets = ("FAANG Legacy Experiments");
#the value for standardMet according to the ruleset, keys are expected to include all values in the @rulesets
my %standards = ("FAANG Experiments"=>"FAANG","FAANG Legacy Experiments"=>"FAANG Legacy");
my %validationResult = &validateTotalExperimentRecords(\%experiments,\@rulesets);
my %exp_validation;

OUTER:
foreach my $exp_id (sort {$a cmp $b} keys %experiments){
  my %exp_es = %{$experiments{$exp_id}};
  foreach my $ruleset(@rulesets){
    if($validationResult{$ruleset}{detail}{$exp_id}{status} eq "error"){
      print ERR "$exp_id\tExperiment\terror\t$validationResult{$ruleset}{detail}{$exp_id}{message}\n";
    }else{
      $exp_validation{$exp_id} = $standards{$ruleset};
      $exp_es{standardMet} = $standards{$ruleset};
      #move the insertion codes out the loop to allow insertion of even invalid experiments
      eval{
        $es->index(
          index => $es_index_name,
          type => 'experiment',
          id => $exp_id,
          body => \%exp_es
        );
      };
      if (my $error = $@) {
        die "error indexing experiment in $es_index_name index:".$error->{text};
      }
      last; #if is FAANG standard, no need to deal with FAANG Legacy
    }
  }
}

#insert file into elasticsearch only when the corresponding experiment is valid
#trapping error: the code can continue to run even after the die or errors, and it also captures the errors or dieing words.
foreach my $file_id(keys %files){
  my %es_doc = %{$files{$file_id}};
  my $exp_id = $es_doc{experiment}{accession};
  next unless (exists $exp_validation{$exp_id}); #for now only files linked to valid experiments are allowed into the data portal
  $es_doc{experiment}{standardMet} = $exp_validation{$exp_id} if (exists $exp_validation{$exp_id});
#   print Dumper(\%es_doc);
  eval{
    $es->index(
      index => $es_index_name,
      type => 'file',
      id => $file_id,
      body => \%es_doc
    );
  };
  if (my $error = $@) {
    die "error indexing file in $es_index_name index:".$error->{text};
  }
  $indexed_files{$file_id} = 1;
}

#now %datasets contain the following data:
#under each dataset id key, the value is the corresponding dataset basic information
#under the conserved tmp key, the value is another hash with dataset id as keys, and another hash having specimen, file, experiment information
#deal with datasets
foreach my $dataset_id (keys %datasets){
  next if ($dataset_id eq "tmp");
  my %es_doc_dataset = %{$datasets{$dataset_id}};
  my %exps = %{$datasets{tmp}{$dataset_id}{experiment}};
  my %only_valid_exps;
  #determine dataset standard using the lowest experiment standard
  my $dataset_standard = "FAANG";
  foreach my $exp_id(keys %exps){
    if (exists $exp_validation{$exp_id}){
      $dataset_standard = "FAANG Legacy" if ($exp_validation{$exp_id} eq "FAANG Legacy");
      $only_valid_exps{$exp_id} = $exps{$exp_id};
    }else{ #not valid at all
#      print "Invalid experiment $exp_id, excluded from $dataset_id\n";
    }
  }
  my $num_valid_exps = scalar keys %only_valid_exps;
  if ($num_valid_exps == 0){ #the dataset has no valid experiments, so skipped
    print "dataset $dataset_id has no valid experiments, skipped.\n";
    next;
  }

  $es_doc_dataset{standardMet} = $dataset_standard;
#  print "Stardard $dataset_standard\n".(scalar keys %only_valid_exps)." valid experiments\n";
  #convert some sub-element from hash to array (hash to guarantee uniqueness)
  my %specimens = %{$datasets{tmp}{$dataset_id}{specimen}};
  my %species;
  my @specimens;
  foreach my $specimen(keys %specimens){
    my $specimen_detail = $existingBiosamples{$specimen};
    my %es_doc_specimen = (
      biosampleId => $$specimen_detail{biosampleId}
#      material => $$specimen_detail{material},
#      cellType => $$specimen_detail{cellType},
#      organism => $$specimen_detail{organism}{organism},
#      sex => $$specimen_detail{organism}{sex},
#      breed => $$specimen_detail{organism}{breed}
    );
    push(@specimens,\%es_doc_specimen);
#    $species{$$specimen_detail{organism}{organism}{text}} = $$specimen_detail{organism}{organism};
  }
  @{$es_doc_dataset{specimen}} = sort {$$a{biosampleId} cmp $$b{biosampleId}} @specimens;
  @{$es_doc_dataset{species}} = values %species;
  my @fileArr = values %{$datasets{tmp}{$dataset_id}{file}};
  my @valid_files;
  foreach my $fileEntry(sort {$$a{name} cmp $$b{name}} @fileArr){
    my $file_id = $$fileEntry{fileId};
    push (@valid_files,$fileEntry) if (exists $indexed_files{$file_id});
  }
  @{$es_doc_dataset{file}} = @valid_files;
  @{$es_doc_dataset{experiment}} = values %only_valid_exps;
  @{$es_doc_dataset{instrument}} = keys %{$datasets{tmp}{$dataset_id}{instrument}};
  @{$es_doc_dataset{archive}} = sort {$a cmp $b} keys %{$datasets{tmp}{$dataset_id}{archive}};

  #insert into ES
  eval{
    $es->index(
      index => $es_index_name,
      type => 'dataset',
      id => $dataset_id,
      body => \%es_doc_dataset
    );
  };
  if (my $error = $@) {
    die "error indexing dataset in $es_index_name index:".$error->{text};
  }
}

#investigation work
#print out content of fields to check how to predict the technology used in the dataset
sub investigateFieldContent(){
  my @results = qw/study read_run read_experiment read_study/;
  my %fields;
  push (@{$fields{study}},"study_description");
  push (@{$fields{"read_experiment"}},"description");
  push (@{$fields{"read_study"}},"description");
  my @readRunFields = qw/study_title library_strategy description library_selection library_layout library_source experiment_title/;
  @{$fields{"read_run"}} = @readRunFields;
  my %text;
  foreach my $result(@results){
    my @fields = @{$fields{$result}};
    my $fields = join(",",@fields);
    foreach my $species(@species){
      my $url = "https://www.ebi.ac.uk/ena/portal/api/search/?result=$result&format=JSON&limit=0&&query=\"tax_eq($species)\"&fields=$fields";
      #print "$url\n\n";
      my $apiResult = &getAPIresult($url);
      foreach my $item(@{$apiResult}){
        foreach my $field(@fields){
          my $value = $$item{$field};
          $text{$value}++;
        }
      }
    }
  }
  foreach (keys %text){
    print "$_\n";
  }
}
#invesigation work
#the most important one
#how to classify datasets into different technologies (categories) based on the values from the fields
#unclassified datasets are stored in the leftover.txt
sub predict(){
  my %result;
  my @results = qw/study read_run read_experiment read_study/;
  my %fields;
  my %existing_study; 
  my %categorised_study; #the studies have been categorised
  my @studyFields = qw/study_accession study_description/;
  @{$fields{"study"}} = @studyFields;
  my @readFields = qw/study_accession description/;
  @{$fields{"read_experiment"}} = @readFields;
  @{$fields{"read_study"}} = @readFields;
#  push (@{$fields{study}},"study_description");
#  push (@{$fields{"read_experiment"}},"description");
#  push (@{$fields{"read_study"}},"description");
  my @readRunFields = qw/study_accession study_title library_strategy description library_selection library_layout library_source experiment_title/;
  @{$fields{"read_run"}} = @readRunFields;
  foreach my $result(@results){
    my @fields = @{$fields{$result}};
    my $fields = join(",",@fields);
    foreach my $species(@species){
      print "Dealing with $result $species\n";
      my $url = "https://www.ebi.ac.uk/ena/portal/api/search/?result=$result&format=JSON&limit=0&query=\"tax_eq($species)\"&fields=$fields";
#      print "$url\n\n";
      my $apiResult = &getAPIresult($url);
      foreach my $item(@{$apiResult}){
        my $study_accession = $$item{"study_accession"};
        #if the study has never been seen before
        #1. add into study list
        #2. assume not categorised
        unless (exists $existing_study{$study_accession}){
          $existing_study{$study_accession}=$species;
        }
        foreach my $field(@fields){
          my $value = $$item{$field};
          foreach my $term(@categories){
#            print "current $term\n";
            if (index(lc($value),lc($term))>-1){ #the term found
#              print "found $term\n";
              my $category = $categories{$term};
              $categorised_study{$study_accession}++;
              $result{$category}{$study_accession}{"$result-$field"} = $value;
              delete $result{noCategory}{$study_accession} if(exists $result{noCategory}{$study_accession});
            }else{
              unless (exists $categorised_study{$study_accession}) {
                $result{noCategory}{$study_accession}{"$result-$field"} = $value;
              }
            }
          }
        }
      }
    }
  }

  foreach my $category(sort {$a cmp $b} keys %result){
    next if ($category eq "noCategory");
    print "Category $category\n";
    my %studies = %{$result{$category}};
    open OUT, ">predict_${category}.txt";
    foreach my $study_accession(sort {$a cmp $b} keys %studies){
      print OUT "Study $study_accession\nEvidence:\n";
      my %evidences = %{$studies{$study_accession}};
      foreach my $evidence (sort {$a cmp $b} keys %evidences){
        print OUT "$evidence: $evidences{$evidence}\n";
      }
      print OUT "\n";
    }
  }

  return unless (exists $result{noCategory});
  my %uncategorised = %{$result{noCategory}};
  open OUT, ">leftover.txt";
  foreach my $study_accession(sort {$a cmp $b} keys %uncategorised){
    print OUT "Study $study_accession\nEvidence:\n";
    my %evidences = %{$uncategorised{$study_accession}};
    foreach my $evidence (sort {$a cmp $b} keys %evidences){
      print OUT "$evidence: $evidences{$evidence}\n";
    }
    print OUT "\n";
  }

}
#get result from ENA API
sub getAPIresult(){
  my ($url) = @_;
  my $browser = WWW::Mechanize->new();
  $browser->credentials('anon','anon');
  $browser->get( $url );
  my $content = $browser->content();
  return if ((length $content)==0);
  my $json = new JSON;
  my $json_text = $json->decode($content);
  return $json_text;
}
#investigation work
#work out which fields used in different result sets and the overlap situation among those fields
sub investigateENAfields(){
  my @results = qw/study read_run read_experiment read_study/;
  my $baseUrl = "https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=ena&result=";
  my %fields;
  my $browser = WWW::Mechanize->new();
  $browser->credentials('anon','anon');
  foreach my $result(@results){
    my $url = "$baseUrl$result";
    $browser->get( $url );
    my $content = $browser->content();
    my @fields = split("\n",$content);
    foreach my $field(@fields){
      $fields{$field}{count}++;
      push (@{$fields{$field}{detail}},$result);
    }
  }
  foreach my $field (sort {$fields{$b}{count} <=> $fields{$a}{count}} keys %fields){
    print "$field\t$fields{$field}{count}\t@{$fields{$field}{detail}}\n";
  }
  exit;
}

sub initializeCategories(){
  my %result;
  $result{"Whole genome sequence"}="WGS";
  $result{"whole genome sequencing"}="WGS";
  $result{"WGS"}="WGS";
  $result{"Whole Genome Shotgun Sequence"}="WGS";

  $result{"ChIP-Seq"}="ChIP-Seq";
  $result{"ChIP-seq Histones"}="ChIP-Seq";

  $result{"Hi-C"}="Hi-C";

  $result{"ATAC-seq"}="ATAC-seq";

  $result{"RNA-Seq"}="RNA-Seq";
  $result{"RNA seq"}="RNA-Seq";
  $result{"miRNA-Seq"}="RNA-Seq";
  $result{"MiSeq"}="Other";
  $result{"ssRNA-seq"}="RNA-Seq";
  $result{"strand-specific RNA sequencing"}="RNA-Seq";
  $result{"Transcriptome profiling"}="RNA-Seq";
  $result{"RNA sequencing"}="RNA-Seq";

  $result{"Bisulfite-Seq"}="BS-Seq";
  $result{"Bisulfite Sequencing"}="BS-Seq";
  $result{"BS-Seq"}="BS-Seq";
  $result{"Whole Genome Bisulfite Sequencing"}="BS-Seq";
  $result{"WGBS"}="BS-Seq";
  $result{"Reduced Representation Bisulfite Sequencing"}="BS-Seq";
  $result{"RRBS"}="BS-Seq";

  $result{"DNase"} = "DNase";

  $result{"GeneChip"}="Other";

  $result{"MeDIP-Seq"}="Other";
  $result{"MeDIP"}="Other";
  $result{"methylated DNA immunoprecipitation-sequencing"}="Other";

  $result{"RIP-Seq"}="Other";
  return %result;
}

sub getFieldsList(){
  my $result;
  my @fieldsToBeImported = qw/study_accession secondary_study_accession sample_accession experiment_accession run_accession submission_accession tax_id instrument_platform instrument_model library_strategy library_selection read_count base_count first_public last_updated study_title study_alias run_alias fastq_bytes fastq_md5 fastq_ftp fastq_aspera fastq_galaxy submitted_format sra_bytes sra_md5 sra_ftp sra_aspera sra_galaxy cram_index_ftp cram_index_aspera cram_index_galaxy project_name/;
  $result = join(",",@fieldsToBeImported);

#more dynamic, but the API developer does not recommend this
#  my $fieldUrl = "https://www.ebi.ac.uk/ena/portal/api/returnFields?dataPortal=ena&result=read_run";
#  my $browser = WWW::Mechanize->new();
#  $browser->credentials('anon','anon');
#  $browser->get($fieldUrl);
#  my $content = $browser->content();
#  my @fields = split("\n",$content);
#  $result = join(",",@fields);
  return $result;
}

sub getFAANGstandardDatasets(){
  my %results;
  #two-step process: first to get how many datasets in the ES
  my $esUrl = "http://$es_host/$es_index_name/dataset/_search?_source=standardMet";
  my $browser = WWW::Mechanize->new();
  $browser->get( $esUrl );
  my $content = $browser->content();
  my $json = new JSON;
  my $json_text = $json->decode($content);
  my $total = $$json_text{hits}{total};
  #second step to set the size to the exact value to avoid missing any records
  $esUrl = $esUrl."&size=$total";
#  print "$esUrl\n";
  $browser->get( $esUrl );
  $content = $browser->content();
  $json_text = $json->decode($content);
  my @hits = @{$$json_text{hits}{hits}};
  foreach my $hit(@hits){
    $results{$$hit{_id}}=1 if ($$hit{_source}{standardMet} eq "FAANG"); #skip FAANG standard which is dealt with in the sibling script
  }
  return %results;
}

#read in BioSample specimen list, keep all information as it will be needed to populate specimen section in dataset
sub getExistingIDs(){
  my ($type) = @_;
#  my %biosample_ids;
  my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => $type,
    search_type => 'scan',
    size => 500,
  );
  while (my $loaded_doc = $scroll->next) {
#    $biosample_ids{$loaded_doc->{_id}}=$$loaded_doc{_source};
    $existingBiosamples{$loaded_doc->{_id}}=$$loaded_doc{_source};
  }
}
#when a BioSample is included in an experiment, that BioSample needs to be in ES, if not, imported
#this subroutine checks the existance of the given BioSample accession and if needed retrieve the data and save into
#two hashes according to material type
sub importBioSample(){
  my ($accession) = @_;
  return if (exists $existingBiosamples{$accession});
  #retrieve data
  my $url = "https://www.ebi.ac.uk/biosamples/samples/$accession";
  my $json_text = &fetch_json_by_url($url);
  #default set to be specimen from organism
  my %material = (
    text => "specimen from organism",
    ontologyTerms => "http://purl.obolibrary.org/obo/OBI_0001479"
  );
  my $confirmedMaterialFlag = 0;
  #check whether exists material field, if so, use that value
  my $materialKey = "Material";#no clue which keywords used in BioSamples, try both
  unless (exists $$json_text{characteristics}{$materialKey}) {
    if (exists $$json_text{characteristics}{material}){
      $materialKey = "material";
    }else{
      $materialKey = "";
    }
  }

  if (length $materialKey > 0){
    %material = %{$$json_text{characteristics}{$materialKey}[0]};
    $confirmedMaterialFlag = 1;
  }
  
  my @parentAnimals;
  my $animal = '';
  #check relationship (derived from, child of) meanwhile assigning material type if not assigned in previous steps
  if (exists $$json_text{relationships}){
    my @relations = @{$$json_text{relationships}};
    foreach my $ref(@relations){
      if ($$ref{type} eq 'child of'){#this relationship only exists in organism (animal)
        if($confirmedMaterialFlag == 0){#material not confirmed
          %material = (
            text => "organism",
            ontologyTerms => "http://purl.obolibrary.org/obo/OBI_0100026"
          );
          $confirmedMaterialFlag = 1;
        }
        push (@parentAnimals,$$ref{target}) unless ($$ref{target} eq $accession); 
      }elsif ($$ref{type} eq 'derived from'){#this relationship can be between (specimen and organism) and (two different types of specimen)
        unless ($$ref{target} eq $accession){
          my %relMaterial;
          if (exists $checkedMaterial{$$ref{target}}){
            %relMaterial = %{$checkedMaterial{$$ref{target}}};
          }else{
            %relMaterial = &importBioSample($$ref{target}) ;
          }
          if ($relMaterial{confirmed}==1 && $relMaterial{material}{text} eq "organism"){
            $confirmedMaterialFlag = 1; #derive from animal, only could be specimen from organism (the default value)
            $animal = $$ref{target};
          }
        }
      }
    }
  }

  #prepare the ES data
  my %es_doc;
  #being a FAANG record does not necessarily mean that it is in the ES due to the condition of meeting FAANG standard
  unless (exists $$json_text{characteristics}{project} && $$json_text{characteristics}{project}[0]{text} eq "FAANG"){
    $es_doc{biosampleId} = $accession;
    %{$es_doc{material}} = %material;
    if ($accession =~/^SAMEA/){
      $es_doc{"id_number"} = substr($accession,5);
    }else{
      if ($accession =~/(\d+)/){
        $es_doc{"id_number"} = -$1;
      }
    }
    $es_doc{description} = $$json_text{characteristics}{description}[0]{text} if (exists $$json_text{characteristics}{description});#V4.0 change
  
    my $organismKey = "organism";
    unless (exists $$json_text{characteristics}{$organismKey}) {
      if (exists $$json_text{characteristics}{Organism}){
        $organismKey = "Organism";
      }else{
        $organismKey = "";
      }
    }
    my %organismES;
    if (length $organismKey > 0){
      %organismES = (
        text => $$json_text{characteristics}{$organismKey}[0]{text},
        ontologyTerms => $$json_text{characteristics}{$organismKey}[0]{ontologyTerms}
      );
    }

    $es_doc{releaseDate} = &parseDate($$json_text{release});
    $es_doc{updateDate} = &parseDate($$json_text{update});
    $es_doc{standardMet} = "FAANG other";
    $es_doc{derivedFrom} = $animal if (length $animal > 0);
    @{$es_doc{childOf}} = @parentAnimals if (scalar @parentAnimals > 0);
    my @customs;
    my %characteristics = %{$$json_text{characteristics}};
    foreach my $name (keys %characteristics){
      next if ($name eq "description");
      next if ($name eq $organismKey);
      next if ($name eq $materialKey);
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
    @{$es_doc{customField}}=@customs;


    print Dumper(\%es_doc);
    #import into ES
    my $type;
    if ($es_doc{material}{text} eq "organism"){
      $type = "organism";
      %{$es_doc{organism}} = %organismES;
    }else{
      $type = "specimen";
      %{$es_doc{organism}{organism}} = %organismES;
      $es_doc{organism}{biosampleId} = $animal if (length $animal>0);
    }
    eval{
      $es->index(
        index => $es_index_name,
        type => $type,
        id => $accession,
        body => \%es_doc
      );
    };
    if (my $error = $@) {
      die "error indexing sample in $es_index_name index".$error->{text};
    }


    #add to the existing biosample list
    $existingBiosamples{$accession} = \%es_doc;
  }

  my %result;
  $result{confirmed} = $confirmedMaterialFlag;
  $result{accession} = $accession;
  %{$result{material}} = %material;
  if ($confirmedMaterialFlag == 1){
    %{$checkedMaterial{$accession}} = %result;
    #according to material type assign to different hashes
  }
#  print "End of import $accession\n";
#  print Dumper(\%result); 
#  print Dumper(\%es_doc);
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
