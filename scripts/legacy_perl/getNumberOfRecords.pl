require "misc.pl";

my @indice = qw/1 2 3 4 5 6 7/;
my @types = qw/organism specimen dataset file experiment/;

my $baseUrl = "http://ves-hx-e4:9200/faang_build_";

foreach my $type(@types){
	print "\t$type";
}
print "\n";
foreach my $idx (@indice){
	print "faang_build_$idx";
	foreach my $type(@types){
		my $url = "$baseUrl$idx/$type/_search?_source=_id";
#		print "$url\n";
		my $json = &fetch_json_by_url($url,1);
		my $count = 0;
		next if (length $json == 0);
		$count = $$json{hits}{total};
		print "\t$count";
	}
	print "\n";
}