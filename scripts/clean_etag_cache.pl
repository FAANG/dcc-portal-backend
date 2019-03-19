use strict;

my $copy_cache = 5;
open CMD, "ls -tl etag_list_*.txt|";
my @records = <CMD>;
for (my $i = $copy_cache; $i < scalar @records; $i++){
	if ($records[$i]=~/(etag_list_\d+-\d+-\d+\.txt)\s*$/){
		my $cmd = "rm $1";
#		print "$cmd\n";
		system($cmd);
	}
}
