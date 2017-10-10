#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Search::Elasticsearch;
use WWW::Mechanize;
use JSON -support_by_pp;
use Data::Dumper;


my $es_host = "ves-hx-e4:9200";
my $es_index_name = 'faang';

GetOptions(
  'es_host=s' =>\$es_host,
  'es_index_name=s' =>\$es_index_name,
);

#croak "Need -es_host" unless ($es_host);
#print "Working on $es_index_name at $es_host\n";


#the line below enable to investigate the fields used in ENA
#&investigateENAfields($json_text);

my $es = Search::Elasticsearch->new(nodes => $es_host, client => '1_0::Direct'); #client option to make it compatiable with elasticsearch 1.x APIs

#get specimen information from current elasticsearch server
#which means that this script must be executed after import_from_biosample.pl
my @types = qw/organism specimen/;
foreach my $type(@types){
#  $type = "specimen";
  &validateAllRecords($type);
#  exit;
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
sub validateAllRecords(){
  my $type = $_[0];
  print "Validating $type data\n";
#  my %biosample_ids;
  my $scroll = $es->scroll_helper(
    index => $es_index_name,
    type => $type,
    search_type => 'scan',
    size => 500,
  );
  my $count = 0;
  open OUT,">tmp2.json";
  print OUT "[\n";
#  my @convertedData;
  while (my $loaded_doc = $scroll->next) {
    my $biosampleId = $$loaded_doc{_id};
#    print "$biosampleId\n";
    $count++;
    my %data = %{$$loaded_doc{_source}};
    my $convertedData = &convert(\%data,$type);
    my $jsonStr = to_json($convertedData);
    print OUT ",\n" unless ($count==1);
    print OUT "$jsonStr\n";
#    last if ($count == 5);
  }
  print OUT "]\n";
  close OUT;
#  exit;
#    my $host = "https://www.ebi.ac.uk/vg/faang/validate";
    #using curl to fill the form, reference page https://curl.haxx.se/docs/manual.html
  my $cmd = 'curl -F "format=json" -F "rule_set_name=FAANG Samples" -F "file_format=JSON" -F "metadata_file=@tmp2.json" "https://www.ebi.ac.uk/vg/faang/validate"';
  my $pipe;
  open $pipe, "$cmd|";
  my $response = &readHandleIntoString($pipe);
  my $json = &decode_json($response);
  &printValidatationResult($$json{entities},$type);
}

sub convert(){
  my %data = %{$_[0]};
  my $type = $_[1];
  my @keys = sort keys %data;
  $"=">,<";
  my @attr;
  my %result;
  $result{entity_type}="sample";
  $result{id}=$data{biosampleId};
  my $material = $data{material}{text};
  #delete the extra keys which are not in the ruleset  
  delete $data{releaseDate};
  delete $data{updateDate};
  delete $data{organization};
  delete $data{biosampleId};
  delete $data{name};

  my %typeSpecific;
  if($type eq "organism"){
    @attr = &parse(\@attr,\%data,$type);
  }else{
    #delete the data sections which were created for sorting purpose in the frontend or similar purpose and are NOT in the rule set
    delete $data{cellType};
    delete $data{organism};
    my $typeSpecific = &toLowerCamelCase($material);
    if (exists $data{$typeSpecific}){
      %typeSpecific = %{$data{$typeSpecific}};
      delete $data{$typeSpecific};
    }else{
      print "Error: type specific data not found for $result{id} (type $material)\n";
      return;
    }
    @attr = &parse(\@attr,\%data,$type);
    @attr = &parse(\@attr,\%typeSpecific,$type);
  }

  @{$result{attributes}}= @attr;
  return \%result;
}

sub parse(){
  my @attr = @{$_[0]};
  my %data = %{$_[1]};
  my $type = $_[2];
  foreach my $key(keys %data){
    next if ($key eq "organization");
    my $refType = ref($data{$key});
    #according to ruleset https://www.ebi.ac.uk/vg/faang/rule_sets/FAANG%20Samples
    #only organism.healthStatus, organism.childOf, specimenFromOrganism.healthStatusAtCollection, poolOfSpecimen.derivedFrom,
    #cellSpecimen.cellType
    if ($refType eq "ARRAY"){
      my %hash;
      my $matched = &fromLowerCamelCase($key);
      $matched = "Child of" if ($key eq "childOf");
      foreach my $elmt(@{$data{$key}}){
        if (ref($elmt) eq "HASH"){
          my %hash = &parseHash($elmt,$matched,$type);
          push(@attr,\%hash);
        }else{
          my %hash = (
            name => $matched,
            value => $elmt
          );
          push(@attr,\%hash);
        }
      }
    }elsif($refType eq "HASH"){
      my %hash = &parseHash($data{$key},$key,$type);
      push(@attr,\%hash);
    }else{
      my %tmp;
      $tmp{name}=&fromLowerCamelCase($key); 
      $tmp{name} = "Sample Description" if ($key eq "description");
      $tmp{name} = "Derived from" if ($key eq "derivedFrom");
      $tmp{value} = $data{$key};
      push(@attr,\%tmp);
    }
  }
  return @attr;
}

sub parseOntologyTerm(){
  my $uri = $_[0];
  my $idx = rindex($uri,"\/");
  my $id = substr($uri,$idx+1);
  $id =~s/:/_/; #some ontology id use : as the separator
  $id = "OBI_0100026" if ($id eq "UBERON_0000468");#some biosample records wrongly assigned the ontology id to organism by the submitter
  my ($source) = split ("_",$id);
  my %result = (
    id => $id,
    source_ref => $source
  );
  return %result;
}
#should be the same as ucfirst
sub capitalizationFirstLetter(){
  my $in = $_[0];
  if (length $in < 2){
    return uc $in;
  }else{
    my $first = substr($in,0,1);
    my $remaining = substr($in,1);
    return uc($first).$remaining;
  }
}

sub parseHash(){
  my %hash = %{$_[0]};
  my $key = $_[1];
  my $type = $_[2];
  my %tmp;
  if (exists $hash{ontologyTerms}){
    %tmp = &parseOntologyTerm($hash{ontologyTerms}) if (length $hash{ontologyTerms});#in 5.12, length $var retur undef when $var undefined
  }
  $tmp{units} = $hash{unit} if (exists $hash{unit});
  if (exists $hash{url}){
    $tmp{value} = $hash{url};
    $tmp{uri} = $hash{url} ;
    $key = &fromLowerCamelCase($key);
  }else{
#    $key = &capitalizationFirstLetter($key);
    if ($key eq "material"){
      $key = "Material";
    }else{
      $key = &fromLowerCamelCase($key);
    }
    $tmp{value} = $hash{text};
  }
  $tmp{name} = $key;
  return %tmp;
}

sub fromLowerCamelCase(){
  my $str = $_[0];
  my @arr = split(/(?=[A-Z])/,$str);
  return lc(join (" ",@arr));
}
#convert a string containing _ or space into lower camel case
sub toLowerCamelCase(){
  my $str = $_[0];
  $str=~s/_/ /g; 
  $str =~ s/^\s+|\s+$//g;
  $str = lc ($str);
  $str =~s/ +(\w)/\U$1/g;
  return $str;
}

sub printValidatationResult(){
  my @entities = @{$_[0]};
  my $type = $_[1];
  my @status = qw/pass warning error/;
  my %summary;
  foreach my $entity(@entities){
    my %hash= %{$entity};
    my $status = $hash{_outcome}{status};
    $summary{$status}++;
    print "$hash{id}\t$type\t$status\t";
    if ($status eq "pass"){
      print "\n";
      next;
    }
    #if the warning related to columns not existing in the data, the following attribute iteration will not go through that column
    my $backupMsg = "";
    my $tag = $status."s";
    $backupMsg = join (";",@{$hash{_outcome}{$tag}}) if (exists $hash{_outcome}{$tag});
    my @msgs;
    my @attributes = @{$hash{attributes}};
    foreach my $attr (@attributes){
      next if ($$attr{_outcome}{status} eq "pass");
      $tag = $$attr{_outcome}{status}."s";
      my $msg = "$$attr{name}:".$$attr{_outcome}{$tag}[0];
      push (@msgs,$msg);
    }
    my $totalMsg = join (";",@msgs);
    $totalMsg = $backupMsg if (scalar @msgs == 0);
    print "$totalMsg\n";
  }
  print "Summary:\n";
  foreach (@status){
    if (exists $summary{$_}){
      print "$_\t$summary{$_}\n";
    }else{
      print "$_\t0\n";
    }
  }
  print "\n";
}