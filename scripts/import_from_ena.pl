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

GetOptions(
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
);

croak "Need -es_host" unless ($es_host);
#print "Working on $es_index_name at $es_host\n";
my $ruleset_version = &getRulesetVersion();
print "Rule set release: $ruleset_version\n";

#Import FAANG data from FAANG endpoint of ENA API
#ENA API documentation available at: http://www.ebi.ac.uk/ena/portal/api/doc?format=pdf
my $url = "https://www.ebi.ac.uk/ena/portal/api/search/?result=read_run&format=JSON&limit=0&dataPortal=faang&fields=all";
my $browser = WWW::Mechanize->new();
$browser->credentials('anon','anon');
$browser->get( $url );
my $content = $browser->content();
my $json = new JSON;
my $json_text = $json->decode($content);

#the line below enable to investigate the fields used in ENA
#&investigateENAfields($json_text);

my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct'); #client option to make it compatiable with elasticsearch 1.x APIs

#get specimen information from current elasticsearch server
#which means that this script must be executed after import_from_biosample.pl
my %biosample_ids = &getAllSpecimenIDs();
croak "BioSample IDs were not imported" unless (%biosample_ids);

my $error_record_file = "ena_not_in_biosample.txt";
my %known_errors;
my %new_errors;
open IN, "$error_record_file";
while (my $line=<IN>){
  chomp($line);
  my ($study,$biosample) = split("\t",$line);
  $known_errors{$study}{$biosample} = 1;
}

#used for deleting no longer existant ES records, e.g. record with old id system
my %indexed_files;

my @data_sources = qw/fastq sra cram_index/;
my @data_types = qw/ftp galaxy aspera/;

foreach my $record (@$json_text){
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
      organism => $biosample_ids{$$record{sample_accession}}{organism},
      species => $biosample_ids{$$record{sample_accession}}{species},
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
        assayType => $$record{assay_type},
        target => $$record{experiment_target}
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
    #insert into elasticsearch
    #trapping error: the code can continue to run even after the die or errors, and it also captures the errors or dieing words.
#    my $id = "$$record{sample_accession}-$files[$i]";
    eval{
      $es->index(
        index => $es_index_name,
        type => 'file',
        id => $filename,
        body => \%es_doc
      );
    };
    if (my $error = $@) {
      die "error indexing sample in $es_index_name index:".$error->{text};
    }
    $indexed_files{$filename} = 1;

#    print Dumper(\%es_doc);
#    print "\n";
  }
}

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

#read the content from the file handle and concatenate into a string
#for development purpose of reading several records from a file
sub readHandleIntoString(){
        my $fh = $_[0]; 
        my $str = "";
        while (my $line = <$fh>) {
                chomp($line);
                $str .= $line;
        }
        return $str;
}

#read in BioSample specimen list
sub getAllSpecimenIDs(){
  my %biosample_ids;
  my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => 'specimen',
    search_type => 'scan',
    size => 500,
  );
  while (my $loaded_doc = $scroll->next) {
    my %tmp;
    $tmp{species} = $$loaded_doc{_source}{organism}{organism};
    $tmp{organism} = $$loaded_doc{_source}{organism}{biosampleId};
    $biosample_ids{$loaded_doc->{_id}}=\%tmp;
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

sub convertReadable(){
  my @units = qw/B kB MB GB TB PB/;
  my $size = $_[0];
  my $i;
  for ($i=0;$i<6;$i++){
    $size /=1024;
    last if $size<1;
  }
  $size *=1024;
  return "${size}B" if ($i==0);
  my $out = sprintf('%.2f', $size);
  $out.=$units[$i];
  return $out;
}