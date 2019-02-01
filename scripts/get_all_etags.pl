require "misc.pl";


my $url = "https://www.ebi.ac.uk/biosamples/accessions?project=FAANG&limit=100000";
my @biosample_ids = &fetch_biosamples_ids($url);
print "Start:".localtime."\n";
my $today = &get_today();
open OUT, ">etag_list_$today.txt";
foreach my $acc(@biosample_ids){
	my $etag = &fetch_etag_biosample_by_accession($acc);
	print OUT "$acc\t$etag\n";
}
close OUT;
print "Finish:".localtime."\n";

sub fetch_biosamples_ids(){
  my ($url) = @_;
  my $json_text = &fetch_json_by_url($url);
  my @ids = @{$json_text};
  return @ids;
}

