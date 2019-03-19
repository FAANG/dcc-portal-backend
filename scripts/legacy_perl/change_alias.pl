#!/usr/bin/perl
use strict;
use JSON;
use Data::Dumper;
use LWP::UserAgent;
use Getopt::Long;
use Carp;

my $es_host = "ves-hx-e4:9200";
my $es_index;
my $faang_alias = "faang";

#Parse the command line options
#Example: perl import_from_biosamples.pl -project faang -es_host <elasticsearch server> -es_index_name faang
GetOptions(
  'es_host=s' =>\$es_host,
  'es_index=s' =>\$es_index,
  'es_alias=s' =>\$faang_alias,
);
croak "Need -es_index e.g. faang_build_1 which will be the new target index of the alias or the specific value of CURRENT to print current alias" unless ( $es_index);

my $term;
open $term, "curl -XGET \'$es_host/_alias\'|";
my $json = decode_json(&readHandleIntoString($term));
my %current = %$json;
die "Could not find the specified index $es_index at server $es_host" unless (exists $current{$es_index} || $es_index eq "CURRENT");
my %in_use;
foreach my $name(keys %current){
	my %alias = %{$current{$name}{aliases}};
	foreach my $alias(keys %alias){
		#print "<$name> <$alias>\n";
		$in_use{$name} = 1 if ($alias eq $faang_alias);
	}
}

if ($es_index eq "CURRENT"){
	my @in_use = sort {$a cmp $b} keys %in_use;
	my $in_use = join (",",@in_use);
	print "The current alias is $in_use\n";
	exit;
}

if (exists $in_use{$es_index}){
	print "Index $es_index is already assigned with the alias $faang_alias\n";
	exit;
}

print "curl -XPUT $es_host/$es_index/_alias/$faang_alias\n";
system ("curl -XPUT $es_host/$es_index/_alias/$faang_alias");
foreach my $curr(keys %in_use){
	print "\ncurl -XDELETE $es_host/$curr/_alias/$faang_alias\n";
	system ("curl -XDELETE $es_host/$curr/_alias/$faang_alias");
}
print "\n";

#read the content from the file handle and concatenate into a string
sub readHandleIntoString(){
	my $fh = $_[0];	
	my $str = "";
	while (my $line = <$fh>) {
		chomp($line);
		$str .= $line;
	}
	return $str;
}
