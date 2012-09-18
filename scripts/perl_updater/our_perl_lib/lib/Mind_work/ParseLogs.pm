package ParseLogs;

use warnings;
use strict; 
$| = 1;

use Cwd 'abs_path';
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Log::Log4perl qw(:easy);

use Definitions ':all';
my $config = MindCommons::xmlfile_to_hash("config.xml");

my $path_files = abs_path("xxx/zzz");
my $header = {};
my $body = {};
my $others;
my @ignored;

sub add_document{
	my $file = shift;
	my @text = read_file( $file ) ;
#	return if $file ne "d:/temp/rtslogs/rts_logs/q";
	print "$file\n";
	parse_file(\@text);
}

sub parse_file_mind {
	my $text = shift;
	my $count = 0;
	my $block;
	foreach my $line (@$text){
#		print "$count\r" if ! (++$count % 500);
		if ($line =~ m/^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d,\d{2,3})(.*)$/){
			if (defined $block && $block !~ m/^\s*$/) {
				parse_block($block);
			}
			$block = $line;
		} else {
			$block .= $line;
		}
	}
	parse_block($block);
}

sub parse_file_in {
	my $text = shift;
	my $count = 0;
	my $block;
	foreach my $line (@$text){
#		print "$count\r" if ! (++$count % 500);
		if ($line =~ m/^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d,\d{2,3})\s+([a-z]+):\s+ ([^\s]) \[DialogicMainThread\] [-]+ EVENT START [-]+$/i){
			if (defined $block && $block !~ m/^\s*$/) {
				parse_block_in($block);
			}
			$block = $line;
		} else {
			$block .= $line;
		}
	}
	parse_block_in($block);
}

sub parse_logs {
    my $fileid = shift;
    $0 = "parse_logs_$0";
#     my $hash = $dbh->getFile($fileid);
#     my $cust_name = $dbh->get_customer_name($hash->{customer_id});
#     my $host_name = $dbh->get_host_name($hash->{host_id});
#     my $filename = $hash->{file_name};
#     my ($name,$dir,$suffix) = fileparse($filename, qr/\.[^.]*/);
#     my ($table_name, $app, $type) = find_table_name("$name$suffix");
#     $table_name .= "_".lc($cust_name);
#     $table_name = substr($table_name, 0, 64);  ## 64 is the max size of table name
#     if (! defined $type || $type ne "info") {
# 	DEBUG "Probably not for $0: $name$suffix from $cust_name, machine $host_name.\n";
# 	return EXIT_IGNORE;
#     }
#     return EXIT_NO_FILE if (! -f $filename);
# 
#     open (MYFILE, "$filename") or LOGDIE  "Couldn't open $filename: $!";
#     INFO "done logs\n";
#     close (MYFILE); 
#     return EXIT_IGNORE;
}


sub logparser_worker {
    my $fileid = shift;
    $0 = "parse_logs_$0";
return IGNORE;
#     my $hash = $dbh->getFile($fileid);
#     my $cust_name = $dbh->get_customer_name($hash->{customer_id});
#     my $host_name = $dbh->get_host_name($hash->{host_id});
#     my $filename = $hash->{file_name};
#     my ($name,$dir,$suffix) = fileparse($filename, qr/\.[^.]*/);
#     my ($table_name, $app, $type) = find_table_name("$name$suffix");
#     $table_name .= "_".lc($cust_name);
#     $table_name = substr($table_name, 0, 64);  ## 64 is the max size of table name
#     if (! defined $type || $type ne "info") {
# 	DEBUG "Probably not for $0: $name$suffix from $cust_name, machine $host_name.\n";
# 	return IGNORE;
#     }
#     return EXIT_NO_FILE if (! -f $filename);
# 
#     open (MYFILE, "$filename") or LOGDIE  "Couldn't open $filename: $!";
#     INFO "done logs\n";
#     close (MYFILE); 
#     return IGNORE;
}

sub start {
}

sub finish {
}

return 1;
