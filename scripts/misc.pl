#!/usr/bin/env perl

use strict;
use warnings;

#read the content from the file handle and concatenate into a string
#could be used for development purpose of reading several records from a file
#or curl ****| to get the web resource
sub readHandleIntoString(){
  my $fh = $_[0]; 
  my $str = "";
  while (my $line = <$fh>) {
    chomp($line);
    $str .= $line;
  }
  return $str;
}

#convert phrase in lower camel case to words separated by space
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

1;