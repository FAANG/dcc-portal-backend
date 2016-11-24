#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Data::Dumper;
use WWW::Mechanize;
use JSON -support_by_pp;

my ($project);

GetOptions(
  "project=s" => \$project
);

croak "Need -project" unless ( $project);

my @samples = fetch_specimens_by_project($project);

sub fetch_specimens_by_project {
  my ( $project_keyword ) = @_;
  
  my $url = "https://www.ebi.ac.uk/biosamples/api/samples/search/findByText?text=project_crt:".$project_keyword;
  
  my $json_text = fetch_biosamples_json($url);
  my $specimens;
  foreach my $sample (@{$json_text->{_embedded}->{samples}}){
    print Dumper($sample);
    print "\n\n\n\n\n";
  }
}

sub fetch_biosamples_json{
  my ($json_url) = @_;
  my $browser = WWW::Mechanize->new();
  $browser->get( $json_url );
  my $content = $browser->content();
  my $json = new JSON;
  my $json_text = $json->decode($content);
  return $json_text;
}