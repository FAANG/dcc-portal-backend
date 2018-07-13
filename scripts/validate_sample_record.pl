#!/usr/bin/env perl

use strict;
use warnings;
use JSON -support_by_pp;

require "misc.pl";

sub validateRecord(){
  my @data = @{$_[0]};
  my $type = $_[1];
  my $ruleset = $_[2];
  my $tmpOutFile = "${type}_records.json";#the temp middle file
  my $count = 0;
  #the temporary middle output file contains all record from one type
  open OUT,">$tmpOutFile";
  #the top level expected by the validate API is an array. each element is a BioSample record
  print OUT "[\n";
  foreach my $data(@data){
    $count++;
    my $convertedData = &convert($data,$type);
    #convert the middle data structure to json for outputting into the temp file
    my $jsonStr = to_json($convertedData);
    print OUT ",\n" unless ($count==1);
    print OUT "$jsonStr\n";
  }
  print OUT "]\n";
  close OUT;
  #using curl to fill the form, reference page https://curl.haxx.se/docs/manual.html
  my $cmd = 'curl -F "format=json" -F "rule_set_name='.$ruleset.'" -F "file_format=JSON" -F "metadata_file=@'.$tmpOutFile.'" "https://www.ebi.ac.uk/vg/faang/validate"';
#  print "\n\n$cmd\n\n";
  my $pipe;
  open $pipe, "$cmd 2> /dev/null|"; #send the file to the server and receive the response
  my $response = &readHandleIntoString($pipe);
  my $json = &decode_json($response);
  return &parseValidatationResult($$json{entities},$type);
}

#convert ES record to the data structure expected by the API
#a hash which has id, entity_type and attributes as its keys (validate-metadata repo Bio/Metadata/Entity)
#most fields should be saved as one element in the attributes array
#every element expects name, value, units, uri and id (Bio/Metadata/Attribute.pm) 
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
  delete $data{standardMet};
  delete $data{versionLastStandardMet};

  my %typeSpecific;
  if($type eq "organism"){
    @attr = &parse(\@attr,\%data,$type);
  }else{
    #delete the data sections which were created for sorting purpose in the frontend or similar purpose and are NOT in the rule set
    #the following two are specimen only
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
#    next if ($key eq "organization");
    my $refType = ref($data{$key});
    #according to ruleset https://www.ebi.ac.uk/vg/faang/rule_sets/FAANG%20Samples
    #only organism.healthStatus, organism.childOf, specimenFromOrganism.healthStatusAtCollection, poolOfSpecimen.derivedFrom,
    #cellSpecimen.cellType are of array type (i.e. having multiple values)
    #for array, each element has a corresponding entry directly under attributes array, not forming an array of the same field
    if ($refType eq "ARRAY"){
      my %hash;
      my $matched = &fromLowerCamelCase($key);
      $matched = "Child of" if ($key eq "childOf");
      foreach my $elmt(@{$data{$key}}){
        if (ref($elmt) eq "HASH"){
          my %hash = &parseHash($elmt,$matched,$type);#the hash itself, value used as name, organism or specimen
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
    }else{#scalar
      my %tmp;
      $tmp{name}=&fromLowerCamelCase($key); 
      #these hard-coding are for the fields with special capitalization case or conserved field name in the rule set (check the name field)
      #https://github.com/FAANG/faang-metadata/blob/master/rulesets/faang_samples.metadata_rules.json
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
  my ($source) = split ("_",$id);#ontology library, e.g. PATO, EFO, normally could be extract from ontology id
  #this is only part of the hash which will be populated after being returned (name, value, units)
  my %result = (
    id => $id,
    source_ref => $source
  );
  return %result;
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


sub parseValidatationResult(){
  my @entities = @{$_[0]};
  my $type = $_[1];
  my %summary;
  my %errors;
  my %result;
  foreach my $entity(@entities){
    my %hash= %{$entity};
    my $status = $hash{_outcome}{status};
    $summary{$status}++;
    my $id = $hash{id};
    $result{detail}{$id}{status} = $status;
    $result{detail}{$id}{type} = $type;
    #if the warning/error related to columns is "not existing in the data" (e.g. no project column found), the following attribute iteration will not go through that column
    my $backupMsg = "";
    my $tag = $status."s";
    my @outcomeMsgs = @{$hash{_outcome}{$tag}};
    if (exists $hash{_outcome}{$tag}){
      for (my $i=0;$i<scalar @outcomeMsgs;$i++){
        $outcomeMsgs[$i] = "($status)$outcomeMsgs[$i]";
      }
    }
    $backupMsg = join (";",@outcomeMsgs);

    my @msgs;
    my @attributes = @{$hash{attributes}};
    my $bothTypeFlag = 0; #turn to 1 if containing both error and warning
    my $containErrorFlag = 0;
    foreach my $attr (@attributes){
      my $fieldStatus = uc($$attr{_outcome}{status});
      next if ($fieldStatus eq "PASS");
      $bothTypeFlag = 1 if ($fieldStatus ne $status);
      $containErrorFlag = 1 if ($fieldStatus eq "ERROR");
      $tag = lc($fieldStatus)."s";
      my $msg = "$$attr{name}:".$$attr{_outcome}{$tag}[0];
      $errors{$msg}++ if($fieldStatus eq "ERROR");#only want error message not the warning
      $msg = "($fieldStatus)".$msg;
      push (@msgs,$msg);
    }
    @msgs = sort @msgs;
    my $totalMsg = join (";",@msgs);
    if (scalar @msgs == 0){
      $totalMsg = $backupMsg;
#      $errors{$backupMsg}++;
      $errors{$backupMsg}++ if($status eq "error");#only want error message
    }elsif ($bothTypeFlag == 1 && $containErrorFlag == 0){
      $totalMsg .= ";$backupMsg";
    }
    $result{detail}{$hash{id}}{message} = $totalMsg;
  }
  %{$result{summary}}=%summary;
  %{$result{errors}}=%errors;
  return %result;
}

#retrieve release number from GitHub via API
sub getRulesetVersion(){
  my $cmd = 'curl https://api.github.com/repos/FAANG/faang-metadata/releases';
  my $pipe;
  open $pipe, "$cmd 2> /dev/null|";
  my $response = &readHandleIntoString($pipe);
  my $json = &decode_json($response);
  my $current = $$json[0];
  return $$current{tag_name};
}
#merge the validation result of one portion into the total result
sub mergeResult(){
  my %totalResults = %{$_[0]};
  my %partResults = %{$_[1]};
  my $ruleset = $_[2];
  my %subResults;
  %subResults = %{$totalResults{$ruleset}} if (exists $totalResults{$ruleset});

  if (exists $subResults{summary}){
    #deal with three parts one by one
    #merge the summary section
    my @status = qw/pass warning error/;
    foreach (@status){
      if (exists $subResults{summary}{$_} && exists $partResults{summary}{$_}){
        $subResults{summary}{$_} += $partResults{summary}{$_};
      }elsif (exists $subResults{summary}{$_}){
      }elsif (exists $partResults{summary}{$_}){
        $subResults{summary}{$_} = $partResults{summary}{$_};
      }else{
        $subResults{summary}{$_} = 0;
      }
    }
    #merge the detail section
    #no need to do existance check, as the input ids are stored in the hash, which guarantees that one record only gets validated once
    foreach my $tmp (keys %{$partResults{detail}}){
      $subResults{detail}{$tmp} = $partResults{detail}{$tmp};
    }
    #merge the error summary
    my %newErrorMessages = %{$partResults{errors}};
    foreach my $msg(keys %newErrorMessages){
      if (exists $subResults{errors}{$msg}){
        $subResults{errors}{$msg} += $newErrorMessages{$msg};
      }else{
        $subResults{errors}{$msg} = $newErrorMessages{$msg};
      }
    }
  }else{ #no summary section, means subResults is empty (i.e. first portion of result for the given ruleset)
    %subResults = %partResults;
  }
  %{$totalResults{$ruleset}} = %subResults;
  return \%totalResults;
}

#split the total records into batches of 1000 records (avoid the timeout error)
#validate each batch of records
#and merged the validation results
#input: 
# 1) a hash with id as its keys and corresponding data in hash/JSON as its values
# 2) the type of the sample: either organism or specimen
#return a hash having three keys: summary, detail and errors
#for the "summary" key, the value is a hash with fixed keys: pass, warning and error with the count as their values
#for the "detail" key, the value is the hash with id (as input) as its keys and error/warning messages as the values
#for the 'errors' key, the value is the hash of error message and its occurrence
sub validateTotalSampleRecords(){
  my %data = %{$_[0]};
  my $type = $_[1];
  my @rulesets = @{$_[2]};
  my %totalResults;
  my $portionSize = 600;
  my @data = sort keys %data;
  my $totalSize = scalar @data;
  my $numPortions = ($totalSize - $totalSize%$portionSize)/$portionSize;
  
########################
# The code below is for debugging purpose to cherry pick one or several records to do the validation combining with setting the type in validate_from_es.pl
#  my @tmp;
#  push (@tmp,$data{"SAMEA3540916"});
#  foreach my $ruleset(@rulesets){
#    my %validationResults = &validateRecord(\@tmp,$type,$ruleset);
#    %totalResults = %{&mergeResult(\%totalResults,\%validationResults,$ruleset)};
#  }
#  print Dumper(\%totalResults);
#  exit;

###########################
#the commented codes in the next section is to debug a portion of records containing a particular accession
#  my $flag = 0;
  for (my $i=0;$i<$numPortions;$i++){
    my @part;
    for (my $j=0;$j<$portionSize;$j++){
      my $biosampleId = pop @data;
      push (@part,$data{$biosampleId});
#      $flag = 1 if ($biosampleId eq "SAMEA103887540");
    }
#    print Dumper(\%totalResults) if ($flag==1);
#    if ($flag==1){
    foreach my $ruleset(@rulesets){
      my %validationResults = &validateRecord(\@part,$type,$ruleset);
      %totalResults = %{&mergeResult(\%totalResults,\%validationResults,$ruleset)};
#      if ($flag == 1){
#        print "$ruleset\nindividual results\n";
#        print Dumper(\%validationResults);
#        print "\nAfter merge\n";
#        print Dumper(\%totalResults);
#     }
    }
#    exit if($flag == 1);
#    last;
#    }
  }

  #deal with the remaining records
  my @part;
  while(my $biosampleId = pop @data){
    push (@part,$data{$biosampleId});
  }
  foreach my $ruleset(@rulesets){
    my %validationResults = &validateRecord(\@part,$type,$ruleset);
    %totalResults = %{&mergeResult(\%totalResults,\%validationResults,$ruleset)};
  }

  return %totalResults;
}

1;