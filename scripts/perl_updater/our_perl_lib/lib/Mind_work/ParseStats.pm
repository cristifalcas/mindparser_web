package ParseStats;

use warnings;
use strict; 
$| = 1;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Log::Log4perl qw(:easy);
use File::Basename;
use Time::HiRes qw( usleep tv_interval gettimeofday);
use Time::Local;
use File::Path qw(make_path remove_tree);
use File::Copy;

use Definitions ':all';
my $config = MindCommons::xmlfile_to_hash("config.xml");

sub finishParser {
    my ($ret, $id, $data) = @_;
}

sub insertStats {
    my ($id, $data, $dbh) = @_;
    $0 = "parse_stats_$0";

    my $cust_name =$dbh->get_host_name($data->{customer_id});
    my $host_name = $dbh->get_host_name($data->{host_id});
    my $filename = $data->{file_name};
    return EXIT_NO_FILE if (! -f $filename);
    my ($name, $dir, $suffix) = fileparse($filename, qr/\.[^.]*/);
    my $table_name = $data->{inserted_in_tablename};

    INFO "Parsing $0 file $name$suffix (id=$data->{id}) as $table_name from cust=$cust_name, host=$host_name.\n";
    open (MYFILE, "$filename") or LOGDIE  "Couldn't open $filename: $!";
    $dbh->createStatsTable($table_name);
    my (@header, $header_hash, $columns, @values);
    my $t0 = [gettimeofday];my $t1=0;my $t2=0;
    my ($count, $second, $extra_arr) = (0, 1, 0);
    while (<MYFILE>) {
	my $line = $_;
	chomp $line;
	my @arr = map {s/(\r)|(^\s*)|(\s*$)//g; $_ } split /,/, $line;
	$count++;
	if ($count == 1) {
	    if ($line !~ m/^Date,\s*Time,/i){
	      close (MYFILE); 
	      return EXIT_WRONG_TYPE ;
	    }
	    @header = @arr;
	    $dbh->add_new_columns ($table_name, \@header);
	} else {
	    ## no support for new fields in middle of the line
	    if ( (scalar @arr) + $extra_arr != scalar @header || $arr[0] !~ m/[0-9]{2}\/[0-9]{2}\/[0-9]{2}/ ){
		DEBUG "filename = $filename\n\trow=$count\n\tarr=".(scalar @arr)."\n\textra=$extra_arr\n\theader_arr=".(scalar @header)."\n\tfirst=$arr[0]\n\tsecond=$arr[1])\n";
		LOGDIE "Strange line nr $count\n\t$line\n\tfrom file $filename\n" if $arr[0] ne "Date";
		next;
	    }
# 
	    my ($d, $m, $y) = split '/', $arr[0]; #21/07/12
	    my ($h, $min, $s) = split ':', $arr[1]; #03:00:15
	    ## timelocal doesn't take GMT in consideration, rrd graph the same. this will insert a wrong time that will be corrected by rrd
	    my $timestamp = timelocal($s,$min,$h,$d,$m-1,$y+2000); ## or timegm
	    unshift @arr, $timestamp;
	    if ($second) {  ## fix header 
	      unshift @header, 'timestamp';
	      $extra_arr += 1; ## 1 from timestamp inserted from date+time
	      my @header_cp = @header;
	      while (my ($index, $val) = each @arr) {
		if ($val =~ m/;/) {
		  my $i=0;
		  my @q = map {$header_cp[$index]."_".$i++} split /;/, $val;
		  $extra_arr += scalar @q - 1; ## 1 from scalar @q replaces 1 element
		  splice(@header, $index, 1, @q); 
		  $dbh->add_new_columns ($table_name, \@q);
		}
	      }
	      $header_hash = $dbh->get_md5_names(@header);
# print Dumper("as",$header_hash,@header);
	      $second = 0;
	    }
	    @arr = map{split /;/} @arr;
	    my $new_vals;
	    ## 1=timestamp, 2=date, 3=time
	    for (my $i = 3; $i < scalar @header; $i++) {
# print Dumper($header_hash->{$header[$i]}, $header_hash, @header, $i);
		$columns .= ",$header_hash->{$header[$i]}" if $count == 2;
		$new_vals .= ", $arr[$i]";
	    }
	    push @values, " ($data->{id}, ".$data->{host_id}.", $arr[0], '$arr[1]', '$arr[2]' $new_vals) ";
	    my $e=tv_interval($t0);$t2 += $e;$t1 += $e;
	    if ($count % 200 == 0){
		$dbh->insertRowsDB($table_name, "(file_id, host_id, $header[0], date, time $columns)", join ",", @values);
		@values = ();
		DEBUG "Insert 200 rows for $0 took $t2 ($name$suffix)\n";$t2=0;
	    };
	    $t0 = [gettimeofday];;
	}
    }
    close (MYFILE);
    $dbh->insertRowsDB($table_name, "(file_id, host_id, $header[0], date, time $columns)", join ",", @values) if scalar @values;
    $dbh->updateFileInfo($data->{id}, $t1, $table_name);
    DEBUG "Total parse $0 for $name$suffix took $t1\n";
    INFO "Done stats $0 for file $name$suffix id $data->{id}\n";
    return START_MUNIN;
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

return 1;
