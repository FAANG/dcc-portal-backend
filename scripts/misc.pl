#!/usr/bin/env perl
#version: 1.2
#last update: 23/08/2018
#1.1 add parseCSVline
#1.1.1 improve trim function
#1.1.2 add getFilenameFromURL
#1.2 add code to retrieve etag according to BioSample id and check whether etag changes 

use strict;
use warnings;
use JSON;
use LWP::UserAgent;
use WWW::Mechanize;
use HTTP::Headers;

#####Testing codes for parseCSVline
#my @strs;
#push (@strs, 'simple, double, triple'); # expect <simple>,<double>,<triple>
#push (@strs, "triple, double, simple"); # expect <triple>,<double>,<simple>
#push (@strs, '"triple, double, simple"'); #expect <triple, double, simple>
#push (@strs, "empty,,   ,end"); #expect <empty>,<>,<>,<end>
#push (@strs, '",something"'); #expect <,something>
#push (@strs, '"   ,another thing"'); #expect <  ,another thing>
#push (@strs, '"wrapped empty,,   ,end"'); #expect <wrapped empty,,  ,end>
#push (@strs, '"wrong'); #expect report error due to quotation mark not in pair
#push (@strs, '"correct"  '); #expect <correct>
#push (@strs, '"wrong again".  '); #expect report error due to . after "
#push (@strs, '"wrong, case 2'); #expect report error due to quotation mark not in pair
#push (@strs, '"first","second"'); #expect <first>, <second>
#push (@strs, '  with spaces ahead, with trailing spaces.  '); #expect <with spaces ahead>, <with trailing spaces>
#push (@strs, '"complicated, yes", "simple", "most, most, difficult"'); #expect <complicated, yes>,<simple>,<most, most, difficult>
#push (@strs, 'single,"double, double", no space in front, "no, space, in, front but within"'); #expect <single>,<double, double>,<no space in front>,<no, space, in, front but within>
#foreach my $str(@strs){
#  print "input value: $str\n";
#  my @result = &parseCSVline($str);
#  print "<@result>\n";
#}
#try to repeat Text::CSV getline function which asks for a file handle
#this is the standalone function working on any given string
#The main algorithm is to split simply by , first, then merge the separated element into one afterwards if needed (wrapped in the same pair of quotation marks)
sub parseCSVline(){
  my $input = $_[0];
  my $len = length $input;
  my @result;
  if ($len <= 1){
    $result[0] = $input;
    return @result;
  }
  my @tmp = split(",",$input);
  my $count = scalar @tmp;
  for (my $i=0;$i<$count;){
    my $curr = $tmp[$i];#first part of final elements
    unless (length &ltrim($curr) == 0){
      my $wrap = "";
      my $first = substr(&ltrim($curr),0,1);
      $wrap = $first if ($first eq "'" || $first eq "\"");
      if ($wrap ne ""){#when there is a starting quotation mark, search for the paired closing quotation mark
        #check self first
        my $tmp = &rtrim($curr);
        my $len = length $tmp;
        my $endChar = substr($tmp,($len-1),1);
        #self contained, i.e. the pair of quotation marks exists in the same element
        unless ($len > 1 && $endChar eq $wrap){#not self contained
          if ($count == 1){ #there is only one element and the element starts with quotation but not finish with, wrong
            die "Cannot parse string <$input> properly" if ($endChar ne $wrap);
          }
          $i++;
          while ($i < $count){
            $ curr .= ",$tmp[$i]";
            $tmp = &rtrim($tmp[$i]);
            if (length $tmp ==0){
              $i++;
              next;
            }
            $endChar = substr($tmp,((length $tmp)-1),1);
            if ($i == $count-1){
              die "Cannot parse string <$input> properly" if ($endChar ne $wrap);
            }
            if ($endChar eq $wrap){
              $curr = &trim($curr);
              last;
            }
            $i++;
          }
        }
      }
    }
    #the commented code below will keep the original whitespace if one element contains only whitespaces
    #now if that is the case, just input an empty string
#   if (length &trim($curr) == 0){
#     push (@result,$curr);
#   }else{
      $curr = &trim($curr);
      $curr =~ s/^\"+|\"+$//g;
      push (@result,$curr);
#   }
    $i++;
  }
  return @result;
    for (my $i=0;$i<scalar @tmp;$i++){
      my $tmp = $tmp[$i];
      $tmp =~s/^\"*//;
      $tmp =~s/\"*$//;
      $result[$i] = $tmp;
    }
    return @result;
}

sub is_etag_changed(){
  my ($accession, $etag) = @_;
  my $url = "http://wwwdev.ebi.ac.uk/biosamples/samples/$accession";
  my $browser = WWW::Mechanize->new();
  $browser->add_header(Accept => 'application/json');
  $browser->add_header("If-None-Match" => $etag);
  $browser->get( $url );
  my $status = $browser->status();
  return 0 if ($status == 304);
  return 1;
}

#get the etag header for the given accession
sub fetch_etag_biosample_by_accession(){
  my ($accession) = @_;
  my $url = "http://wwwdev.ebi.ac.uk/biosamples/samples/$accession";
  my $browser = WWW::Mechanize->new();
  $browser->get( $url );
#  print "Status: ".$browser->status()."\n";
  my $etag = $browser->response()->headers()->header('etag');
  return $etag;
}

#example usage: the links section of JSON on ebi sites
sub fetch_json_by_url(){
  my ($json_url) = @_;

  my $browser = WWW::Mechanize->new();
  #$browser->show_progress(1);  # Enable for WWW::Mechanize GET logging
  $browser->get( $json_url );
  my $content = $browser->content();
  my $json = new JSON;
  my $json_text = $json->decode($content);
  return $json_text;
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
#convert filesize into human readable value
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
sub httpGet(){
  my $url = $_[0];
  my $browser = LWP::UserAgent->new;
  
  my $response = $browser->get( $url );
  die "Can't get $url -- ", $response->status_line
   unless $response->is_success;

  die "Hey, I was expecting HTML, not ", $response->content_type
   unless $response->content_type eq 'text/html';
     # or whatever content-type you're equipped to deal with

  # Otherwise, process the content somehow:
  return $response->content;
}
#do a POST request and return json file
sub httpPost(){
  my ($host,$content) = @_;
  my $ua = LWP::UserAgent->new;
  # set custom HTTP request header fields
  my $req = HTTP::Request->new(POST => $host);
  $req->header('content-type' => 'application/json');
  $req->content($content);
 
  my $resp = $ua->request($req);
  my $jsonResult = "";
  if ($resp->is_success) {
      my $message = $resp->decoded_content;
      #print "Received reply: $message\n";
      $jsonResult = decode_json($message);
  }else{
      print "HTTP POST error code: ", $resp->code, "\n";
      print "HTTP POST error message: ", $resp->message, "\n";
  }
  return $jsonResult;
}
#calculate average of an array
sub average(){
  my @data=@_;
  my $sum = 0;
  my $len = scalar @data;
  foreach my $data(@data){
    $sum += $data;
  }
  return $sum/$len;
}
#remove the flanking whitespaces, equal ltrim+rtrim
sub trim(){
  my $s = shift;
  return unless (defined $s);
  $s =~ s/^\s+|\s+$//g;
  return $s;
}
#remove the starting whitespaces
sub ltrim(){
  my $s = shift;
  $s =~ s/^\s+//g;
  return $s;
}
#remove the trailing whitespaces
sub rtrim(){
  my $s = shift;
  $s =~ s/\s+$//g;
  return $s;
}
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

sub readHandleIntoJson(){
  my $str = &readHandleIntoString($_[0]);
  return decode_json($str);
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