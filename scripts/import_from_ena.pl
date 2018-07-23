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

#my @sizes = qw/822938429 204 3475424256 107868/;
#foreach my $size(@sizes){
#  print "$size\t";
#  print &convertReadable($size);
#  print "\n";
#}

#my $pipe;
#open $pipe,"SAMEA3540911_ena.json" or die "Could not find the file";
#convert into json which is stored in a hash, return the ref to the hash
#my $json_text = decode_json(&readHandleIntoString($pipe));
#print Dumper($json);
#exit;

my $es_host;
my $es_index_name = 'faang';
my $error_log = "import_ena_error.log";

GetOptions(
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
  'error_log=s' =>\$error_log
);

croak "Need -es_host" unless ($es_host);
#print "Working on $es_index_name at $es_host\n";

#my @acc = qw/ERR1017174_1 ERR1017174_2 ERR1017177_1 ERR1017177_2 ERR789177/;
#my %hash;
#foreach my $acc(@acc){
#  my $cmd = "curl $es_host/$es_index_name/file/$acc|";
#  my $fh;
#  open $fh, $cmd;
#  my $result = &readHandleIntoJson($fh);
#  $hash{$acc} = $$result{_source};
#}
#print Dumper(\%hash);
#&convertFilesIntoExperiments(\%hash);
#exit;

#find related samples for the interested studies
#my %manual_studies = (
#  ERP021264 => 1,
#  ERP108707 => 1,
#  ERP023413 => 1,
#  ERP104216 => 1
#);

#Import FAANG data from FAANG endpoint of ENA API
#ENA API documentation available at: http://www.ebi.ac.uk/ena/portal/api/doc?format=pdf
my $url = "https://www.ebi.ac.uk/ena/portal/api/search/?result=read_run&format=JSON&limit=0&dataPortal=faang&fields=all";
my $browser = WWW::Mechanize->new();
$browser->credentials('anon','anon');
$browser->get( $url );
my $content = $browser->content();
my $json = new JSON;
my $json_text = $json->decode($content);

#foreach my $record (@$json_text){
#  print "$$record{secondary_study_accession} : $$record{sample_accession}\n" if (exists $manual_studies{$$record{secondary_study_accession}});
#}

#the line below enable to investigate the fields used in ENA
#&investigateENAfields($json_text);

my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct'); #client option to make it compatiable with elasticsearch 1.x APIs

#get specimen information from current elasticsearch server
#which means that this script must be executed after import_from_biosample.pl
my %biosample_ids = &getAllSpecimenIDs();
croak "BioSample IDs were not imported" unless (%biosample_ids);
#print "Number of specimen in ES: ".(scalar keys %biosample_ids)."\n";
my $error_record_file = "ena_not_in_biosample.txt";
my %known_errors;
my %new_errors;
open IN, "$error_record_file";
while (my $line=<IN>){
  chomp($line);
  my ($study,$biosample) = split("\t",$line);
  $known_errors{$study}{$biosample} = 1;
}

print "The information of invalid records will be stored in $error_log\n\n";
open ERR,">$error_log";

#define the rulesets each record needs to be validated against, in the order of 
my @rulesets = ("FAANG Experiments","FAANG Legacy Experiments");
#the value for standardMet according to the ruleset, keys are expected to include all values in the @rulesets
my %standards = ("FAANG Experiments"=>"FAANG","FAANG Legacy Experiments"=>"FAANG Legacy");
my $ruleset_version = &getRulesetVersion();

#used for deleting no longer existant ES records, e.g. record with old id system
my %indexed_files;

my @data_sources = qw/fastq sra cram_index/;
my @data_types = qw/ftp galaxy aspera/;
#store the cumulative dataset data
#each file has a dataset (study)
#key is either the study accession or fixed value of "tmp". When study accession value is the corresponding es entity
#the tmp key corresponds to another hash with study accession as the keys and temp data structures for files, experiments, species etc.
my %datasets; 
my %experiments;
my %files;

my %strategy;
foreach my $record (@$json_text){
  #it seems that all records share the same set of fields, i.e. no need to check existance

  #hard coded to try to convert to accepted terms/add new fixed fields in FAANG ruleset
  my $library_strategy = $$record{library_strategy};
  my $assay_type = $$record{assay_type};
  my $experiment_target = $$record{experiment_target};
  if ($assay_type eq ""){
    if ($library_strategy eq "Bisulfite-Seq"){
      $assay_type = "methylation profiling by high throughput sequencing";
    }elsif ($library_strategy eq "DNase-Hypersensitivity"){
      $assay_type = "DNase-Hypersensitivity seq";
    }else{
#      print "Cannot predict assay_type for $$record{run_accession}\n";
#      next;
    }
  }
  $assay_type = "whole genome sequencing assay" if ($assay_type eq "whole genome sequencing");

  if($assay_type eq "ATAC-seq"){
    $experiment_target = "open_chromatin_region" unless (length($experiment_target)>0);
  }elsif ($assay_type eq "methylation profiling by high throughput sequencing"){
    $experiment_target = "DNA methylation" unless (length($experiment_target)>0);
  }elsif ($assay_type eq "DNase-Hypersensitivity seq"){
    $experiment_target = "open_chromatin_region" unless (length($experiment_target)>0);
  }elsif ($assay_type eq "Hi-C"){
    $experiment_target = "chromatin" unless (length($experiment_target)>0);
  }elsif ($assay_type eq "whole genome sequencing assay"){
    $experiment_target = "input DNA" unless (length($experiment_target)>0);
  }
#  $strategy{"$library_strategy\t<$assay_type>\t$experiment_target $$record{library_source}"}++;
#  next;
  #dynamically determine which archive DNA file to use, in the order of fastq, sra and cram_index
  #within same archive, ftp is preferred over galaxy and aspera
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
  #by-product of finding specimen with unprocessed DNA files
  if ($file_type eq ""){
#    print "BioSample record $$record{sample_accession} with $$record{run_accession} does not have any processed DNA files\n";
    next; #no file has been found
  }
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
#    print "$$record{sample_accession}\t$$record{run_accession}\t$archive\t$file_type\t$source_type\n";
#    print "$file\n";
    my $specimen_biosample_id = $$record{sample_accession};
    #if the ena records contains biosample records which have not been in FAANG data portal (%biosample_ids) and not been reported before (%known_errors)
    #then these records need to be reported
    unless (exists $biosample_ids{$specimen_biosample_id}){
      $new_errors{$$record{study_accession}}{$specimen_biosample_id} = 1 unless (exists $known_errors{$$record{study_accession}}{$specimen_biosample_id});
      next;
    }
    my $file = $files[$i]; #full path
    my $idx = rindex ($file,"/");
    my $fullname = substr($file,$idx+1);
    $idx = index ($fullname,".");
    my $filename = substr($fullname,0,$idx);
#    print "$filename\n";
#    next;
    my %es_doc = (
      specimen => $specimen_biosample_id,
      organism => $biosample_ids{$$record{sample_accession}}{organism}{biosampleId},
      species => $biosample_ids{$$record{sample_accession}}{organism}{organism},
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
        instrument => $$record{instrument_model},
        centerName => $$record{center_name},
        sequencingDate => $$record{sequencing_date},
        sequencingLocation => $$record{sequencing_location},
        sequencingLatitude => $$record{sequencing_latitude},
        sequencingLongitude => $$record{sequencing_longitude},
      },
      study => {
        accession => $$record{study_accession},
        alias => $$record{study_alias},
        title => $$record{study_title},
        type => $$record{study_type},
        secondaryAccession => $$record{secondary_study_accession}
      }
    );
    %{$files{$filename}} = %es_doc;
    #insert file entry into ES is delayed after validating experiment

    #collect information for experiments
    #assume experiment information would be the same across ENA based on the experiment accession
    my $exp_id = $$record{experiment_accession};
    unless (exists $experiments{$exp_id}){
      my $experiment_protocol = $$record{experimental_protocol};
      my $experiment_protocol_filename = &getFilenameFromURL($experiment_protocol);
      my $extraction_protocol = $$record{extraction_protocol};
      my $extraction_protocol_filename = &getFilenameFromURL($extraction_protocol);
      my %exp_es = (
        accession => $exp_id,
        assayType => $assay_type,
        experimentTarget => $experiment_target,
        sampleStorage => $$record{sample_storage},
        sampleStorageProcessing => $$record{sample_storage_processing},
        samplingToPreparationInterval => {
          text => $$record{sample_prep_interval},
          unit => $$record{sample_prep_interval_units}
        },
        experimentalProtocol => {
          url => $experiment_protocol,
          filename => $experiment_protocol_filename
        },
        extractionProtocol => {
          url => $extraction_protocol,
          filename => $extraction_protocol_filename
        },
        libraryPreparationLocation => $$record{library_prep_location},
        libraryPreparationDate => {
          text => $$record{library_prep_date},
          unit => $$record{library_prep_date_format}
        },
        sequencingLocation => $$record{sequencing_location},
        sequencingDate => {
          text => $$record{sequencing_date},
          unit => $$record{sequencing_date_format}
        }
      );
      if (exists $$record{library_prep_longitude} && length($$record{library_prep_longitude})>0){
        $exp_es{libraryPreparationLocationLongitude}{text} = $$record{library_prep_longitude};
        $exp_es{libraryPreparationLocationLongitude}{unit} = "decimal degrees";
      }
      if (exists $$record{library_prep_latitude} && length($$record{library_prep_latitude})>0){
        $exp_es{libraryPreparationLocationLatitude}{text} = $$record{library_prep_latitude};
        $exp_es{libraryPreparationLocationLatitude}{unit} = "decimal degrees";
      }
      if (exists $$record{sequencing_longitude} && length($$record{sequencing_longitude})>0){
        $exp_es{sequencingLocationLongitude}{text} = $$record{sequencing_longitude};
        $exp_es{sequencingLocationLongitude}{unit} = "decimal degrees";
      }
      if (exists $$record{sequencing_latitude} && length($$record{sequencing_latitude})>0){
        $exp_es{sequencingLocationLatitude}{text} = $$record{sequencing_latitude};
        $exp_es{sequencingLocationLatitude}{unit} = "decimal degrees";
      }
      #reference: https://wwwdev.ebi.ac.uk/vg/faang/rule_sets/FAANG%20Experiments
      my %section_info;
      if ($assay_type eq "ATAC-seq"){
        my $transposase_protocol = $$record{transposase_protocol};
        my $transposase_protocol_filename = &getFilenameFromURL($transposase_protocol);
        %section_info = (
          transposaseProtocol => {
            url => $transposase_protocol,
            filename => $transposase_protocol_filename
          }
        );
        %{$exp_es{"ATAC-seq"}} = %section_info;
      }elsif ($assay_type eq "methylation profiling by high throughput sequencing"){
        my $conversion_protocol = $$record{bisulfite_protocol};
        my $conversion_protocol_filename = &getFilenameFromURL($conversion_protocol);
        my $pcr_isolation_protocol = $$record{pcr_isolation_protocol};
        my $pcr_isolation_protocol_filename = &getFilenameFromURL($pcr_isolation_protocol);
        %section_info = (
          librarySelection => $$record{library_selection},
          bisulfiteConversionProtocol => {
            url => $conversion_protocol,
            filename => $conversion_protocol_filename
          },
          pcrProductIsolationProtocol => {
            url => $pcr_isolation_protocol,
            filename => $pcr_isolation_protocol_filename
          },
          bisulfiteConversionPercent => $$record{bisulfite_percent},
          restrictionEnzyme => $$record{restriction_enzyme}
          #maxFragmentSizeSelectionRange => $$record{},
          #minFragmentSizeSelectionRange => $$record{},
        );
        $section_info{librarySelection} = "RRBS" if (lc($section_info{librarySelection}) eq "reduced representation");
        $section_info{librarySelection} = "RRBS" if (lc($section_info{librarySelection}) eq "size fractionation");
        $section_info{librarySelection} = "WGBS" if (lc($section_info{librarySelection}) eq "whole genome");
        %{$exp_es{"BS-seq"}} = %section_info;
      }elsif($assay_type eq "ChIP-seq"){
        my $chip_protocol = $$record{chip_protocol};
        my $chip_protocol_filename = &getFilenameFromURL($chip_protocol);
        %section_info = (
          chipProtocol => {
            url => $chip_protocol,
            filename => $chip_protocol_filename
          },
          libraryGenerationMaxFragmentSizeRange => $$record{library_max_fragment_size},
          libraryGenerationMinFragmentSizeRange => $$record{library_min_fragment_size}
        );
        if (lc($experiment_target) eq "input dna"){
          %{$exp_es{"ChiP-seq input DNA"}} = %section_info;
        }else{
          $section_info{chipAntibodyProvider} = $$record{chip_ab_provider};
          $section_info{chipAntibodyCatalog} = $$record{chip_ab_catalog};
          $section_info{chipAntibodyLot} = $$record{chip_ab_lot};
          %{$exp_es{"ChiP-seq histone"}} = %section_info;
        }
      }elsif($assay_type eq "DNase-Hypersensitivity seq"){
        my $dnase_protocol = $$record{dnase_protocol};
        my $dnase_protocol_filename = &getFilenameFromURL($dnase_protocol);
        %section_info = (
          dnaseProtocol => {
            url => $dnase_protocol,
            filename => $dnase_protocol_filename
          }
        );
        %{$exp_es{"DNase-seq"}} = %section_info;
      }elsif($assay_type eq "Hi-C"){
        %section_info = (
          restrictionEnzyme => $$record{restriction_enzyme},
          restrictionSite => $$record{restriction_site}
        );
        %{$exp_es{"Hi-C"}} = %section_info;
      }elsif($assay_type eq "whole genome sequencing assay"){
        my $library_pcr_protocol = $$record{library_pcr_isolation_protocol};
        my $library_pcr_protocol_filename = &getFilenameFromURL($library_pcr_protocol);
        my $library_generation_protocol = $$record{library_gen_protocol};
        my $library_generation_protocol_filename = &getFilenameFromURL($library_generation_protocol);
        %section_info = (
          libraryGenerationPcrProductIsolationProtocol => {
            url => $library_pcr_protocol,
            filename => $library_pcr_protocol_filename
          },
          libraryGenerationProtocol => {
            url => $library_generation_protocol,
            filename => $library_generation_protocol_filename
          },
          librarySelection => lc($$record{library_selection})#lc function due to the allowed value is in all lower cases
          #maxFragmentSizeSelectionRange => $$record{},
          #minFragmentSizeSelectionRange => $$record{},
        );

        %{$exp_es{"WGS"}} = %section_info;
#'microRNA profiling by high throughput sequencing'
#'RNA-seq of coding RNA'
#'RNA-seq of non coding RNA'
#'transcription profiling by high throughput sequencing'
#      }elsif($assay_type eq "ChIP-seq"){
      #in the current ruleset, all unprocessed data should belong to RNA-Seq
      }else{
        #no corresponding column found in ENA
        #my $rna_3_adapter_protocol = $$record{};
        #my $rna_3_adapter_protocol_filename = &getFilenameFromURL($rna_3_adapter_protocol);
        my $library_pcr_protocol = $$record{library_pcr_isolation_protocol};
        my $library_pcr_protocol_filename = &getFilenameFromURL($library_pcr_protocol);
        my $rt_protocol = $$record{rt_prep_protocol};
        my $rt_protocol_filename = &getFilenameFromURL($rt_protocol);
        my $library_generation_protocol = $$record{library_gen_protocol};
        my $library_generation_protocol_filename = &getFilenameFromURL($library_generation_protocol);
        %section_info = (
          #rnaPreparation3AdapterLigationProtocol => {
          #  url => $rna_3_adapter_protocol,
          #  filename => $rna_3_adapter_protocol_filename
          #},
          #rnaPreparation5AdapterLigationProtocol => {
          #  url => $rna_5_adapter_protocol,
          #  filename => $rna_5_adapter_protocol_filename
          #},
          libraryGenerationPcrProductIsolationProtocol => {
            url => $library_pcr_protocol,
            filename => $library_pcr_protocol_filename
          },
          preparationReverseTranscriptionProtocol => {
            url => $rt_protocol,
            filename => $rt_protocol_filename
          },
          libraryGenerationProtocol => {
            url => $library_generation_protocol,
            filename => $library_generation_protocol_filename
          },
          readStrand => $$record{read_strand},
          rnaPurity260280ratio => $$record{rna_purity_280_ratio},
          rnaPurity260230ratio => $$record{rna_purity_230_ratio},
          rnaIntegrityNumber => $$record{rna_integrity_num}
        );
        %{$exp_es{"RNA-seq"}} = %section_info;
      }


      %{$experiments{$exp_id}} = %exp_es;
    }#end of unless(exists $experiments{$exp_id})

    #collect information for datasets
    my $dataset_id = $$record{study_accession};
    my %es_doc_dataset;
    if (exists $datasets{$dataset_id}){
      %es_doc_dataset = %{$datasets{$dataset_id}};
    }else{
      #the basic information of dataset which should be same across all files linking to the same dataset
      $es_doc_dataset{accession}=$dataset_id;
      $es_doc_dataset{alias}=$$record{study_alias};
      $es_doc_dataset{title}=$$record{study_title};
      $es_doc_dataset{type}=$$record{study_type};
      $es_doc_dataset{secondaryAccession}=$$record{secondary_study_accession};
    }

    #specimen for dataset
    $datasets{tmp}{$dataset_id}{specimen}{$specimen_biosample_id}=1;
    $datasets{tmp}{$dataset_id}{instrument}{$$record{instrument_model}} = 1;
    $datasets{tmp}{$dataset_id}{centerName}{$$record{center_name}} = 1;
    $datasets{tmp}{$dataset_id}{archive}{$archive} = 1;

    #species can be calculated from specimen information
    #file for dataset
    my %tmp_file = (
      url => $file,
      name => $fullname,
      fileId => $filename,
      experiment => $$record{experiment_accession},
      type => $types[$i],
      size => $sizes[$i],
      readableSize => &convertReadable($sizes[$i]),
#      checksumMethod => "md5",
#      checksum => $checksums[$i],
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

    %{$datasets{$dataset_id}} = %es_doc_dataset;
  }#end of for (@files)
}
print "The processed studies:\n";
my @dataset_ids = sort keys %datasets;
for (my $i=0;$i<scalar @dataset_ids;$i++){
  my $dataset_id = $dataset_ids[$i];
  next if ($dataset_id eq "tmp");
  my %dataset_exps = %{$datasets{tmp}{$dataset_id}{experiment}};
  my $num_exps = scalar keys %dataset_exps;
  print "$i   $dataset_id has $num_exps experiments\n";
}

#finish retrieving the data from ena, now start to validate experiments
#if not valid, no insertion of experiment and related file(s) into ES
my %validationResult = &validateTotalExperimentRecords(\%experiments,\@rulesets);
my %exp_validation;
#print "Total experiment: ".(scalar keys %experiments)."\n";
OUTER:
foreach my $exp_id (sort {$a cmp $b} keys %experiments){
  my %exp_es = %{$experiments{$exp_id}};
  foreach my $ruleset(@rulesets){
    if($validationResult{$ruleset}{detail}{$exp_id}{status} eq "error"){
      print ERR "$exp_id\tExperiment\terror\t$validationResult{$ruleset}{detail}{$exp_id}{message}\n";
    }else{
      $exp_validation{$exp_id} = $standards{$ruleset};
      $exp_es{standardMet} = $standards{$ruleset};
      $exp_es{versionLastStandardMet} = $ruleset_version if ($exp_es{standardMet} eq "FAANG");
      #move the insertion codes out the loop to allow insertion of even invalid experiments
      #eval{
      #  $es->index(
      #    index => $es_index_name,
      #    type => 'experiment',
      #    id => $exp_id,
      #    body => \%exp_es
      #  );
      #};
      #if (my $error = $@) {
      #  die "error indexing experiment in $es_index_name index:".$error->{text};
      #}
      last;
    }
  }
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

}

#insert file into elasticsearch only when the corresponding experiment is valid
#trapping error: the code can continue to run even after the die or errors, and it also captures the errors or dieing words.
foreach my $file_id(keys %files){
  my %es_doc = %{$files{$file_id}};
  my $exp_id = $es_doc{experiment}{accession};
#  next unless (exists $exp_validation{$exp_id}); #for now every file is allowed into the data portal
  $es_doc{experiment}{standardMet} = $exp_validation{$exp_id} if (exists $exp_validation{$exp_id});
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
  #convert some sub-element from hash to array (hash to guarantee uniqueness)
  my %specimens = %{$datasets{tmp}{$dataset_id}{specimen}};
  my %species;
  my @specimens;
  foreach my $specimen(keys %specimens){
    my $specimen_detail = $biosample_ids{$specimen};
    my %es_doc_specimen = (
      biosampleId => $$specimen_detail{biosampleId},
      material => $$specimen_detail{material},
      cellType => $$specimen_detail{cellType},
      organism => $$specimen_detail{organism}{organism},
      sex => $$specimen_detail{organism}{sex},
      breed => $$specimen_detail{organism}{breed}
    );
    push(@specimens,\%es_doc_specimen);
    $species{$$specimen_detail{organism}{organism}{text}} = $$specimen_detail{organism}{organism};
  }
  @{$es_doc_dataset{specimen}} = sort {$$a{biosampleId} cmp $$b{biosampleId}} @specimens;
  @{$es_doc_dataset{species}} = values %species;
#  my @fileArr = values %{$datasets{tmp}{$dataset_id}{file}};
  @{$es_doc_dataset{file}} = sort {$$a{name} cmp $$b{name}} values %{$datasets{tmp}{$dataset_id}{file}};
  @{$es_doc_dataset{experiment}} = values %{$datasets{tmp}{$dataset_id}{experiment}};
  @{$es_doc_dataset{instrument}} = keys %{$datasets{tmp}{$dataset_id}{instrument}};
  @{$es_doc_dataset{centerName}} = keys %{$datasets{tmp}{$dataset_id}{centerName}};
  @{$es_doc_dataset{archive}} = sort {$a cmp $b} keys %{$datasets{tmp}{$dataset_id}{archive}};

  #determine dataset standard using the lowest experiment standard
  my $dataset_standard = "FAANG";
  foreach my $exp_id(keys %{$datasets{tmp}{$dataset_id}{experiment}}){
    if (exists $exp_validation{$exp_id}){
      $dataset_standard = "FAANG Legacy" if ($exp_validation{$exp_id} eq "FAANG Legacy");
    }else{ #not exists, that experiment invalid, then the whole dataset has no standard
      $dataset_standard = "";
      last;
    }
  }
  $es_doc_dataset{standardMet} = $dataset_standard;
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
#print Dumper(\%strategy);
#exit;

open OUT,">>$error_record_file";
foreach my $study(keys %new_errors){
  my %tmp = %{$new_errors{$study}};
  foreach my $biosample(sort keys %tmp){
    print "$biosample from $study does not exist in BioSamples at the moment\n";
    print OUT "$study\t$biosample\n";
  }
}
close OUT;

&clean_elasticsearch();
#delete records in ES which no longer exists in BioSample
#BE careful, this no-more-existances could be caused by lost of server
sub clean_elasticsearch{
  # A scrolled search is a search that allows you to keep pulling results until there are no more matching results, much like a cursor in an SQL database.
  # Unlike paginating through results (with the from parameter in search()), scrolled searches take a snapshot of the current state of the index.
  # scroll: keeps track of which results have already been returned and so is able to return sorted results more efficiently than with deep pagination
  # scan search: disables any scoring or sorting and to return results in the most efficient way possibl
  my $filescroll = $es->scroll_helper(
    index => $es_index_name,
    type => 'file',
    search_type => 'scan',
    size => 500,
  );
  SCROLL:
  while (my $loaded_doc = $filescroll->next) {
    next SCROLL if $indexed_files{$loaded_doc->{_id}};
    $es->delete(
      index => $es_index_name,
      type => 'file',
      id => $loaded_doc->{_id},
    );
  }
}


#read in BioSample specimen list, keep all information as it will be needed to populate specimen section in dataset
sub getAllSpecimenIDs(){
  my %biosample_ids;
  my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => 'specimen',
    search_type => 'scan',
    size => 500,
  );
  while (my $loaded_doc = $scroll->next) {
    $biosample_ids{$loaded_doc->{_id}}=$$loaded_doc{_source};
  }
  return %biosample_ids;
}

sub investigateENAfields(){
  my $json_text = $_[0];  
  my $count = 0;
  my %fields;
  foreach my $record (@$json_text){
    $count++;
    foreach my $field(keys %{$record}){
      $fields{$field}{count}++;
      $fields{$field}{example} = $$record{$field} if ($$record{$field} ne "");
    }
  }
  print "There are in total of $count ENA records\n";
  foreach my $key(sort keys %fields){
    print "$key\t$fields{$key}{count}\t";
    print "$fields{$key}{example}" if (exists $fields{$key}{example});
    print "\n";
  }
  print "\n\nAll messages above are printed from investigateENAfields subroutine which prints the fields used in ENA for FAANG and exits the program. To do the real business, comment the call of this subroutine out.\n";
  exit;
}
