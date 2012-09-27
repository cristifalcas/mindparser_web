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

sub finish {
    my ($ret, $data, $dbh) = @_;
    MindCommons::moveFiles($ret, $data, $dbh);
    $dbh->set_info_for_munin($data);
}

# sub addRows {
#     my ($dbh, $table, $columns, @values);
# print Dumper($table);die;
#     foreach my $row (@values) {
# 	foreach my $val (@$row) {
# 	    $val = $dbh->getQuotedString($val);
# 	}
#     }
# 
# #     $dbh->insertRowsDB($table, $columns, @values);
# }

sub start {
    my ($data, $dbh) = @_;
#     $0 = "parse_stats_$0";
    my $cust_name = $dbh->get_host_name($data->{customer_id});
    my $host_name = $dbh->get_host_name($data->{host_id});
    my $filename = $data->{file_name};
    return EXIT_NO_FILE if (! -f $filename);
    my ($name, $dir, $suffix) = fileparse($filename, qr/\.[^.]*/);
    my $plugin_info = $dbh->getPluginInfo($data->{id});;
    my $table_name = $plugin_info->{inserted_in_tablename};

    INFO "Parsing file $name$suffix (id=$data->{id}) as $table_name from cust=$cust_name, host=$host_name.\n";
    $dbh->createStatsTable($table_name);
    my (@header, $header_hash, @columns, @values);
    my $t0 = [gettimeofday];my $t1=0;my $t2=0;
    my ($count, $second, $extra_arr) = (0, 1, 0);
    open (MYFILE, "$filename") or LOGDIE  "Couldn't open $filename: $!";
    while (<MYFILE>) {
	my $line = $_;
	chomp $line;
	my @arr = map {s/(\r)|(^\s*)|(\s*$)//g; $_ } split /,/, $line;
	next if $line =~ m/^\s*$/;
	$count++;
	if ($count == 1) {
	    if ($line !~ m/^Date,\s*Time,/i){
	      close (MYFILE); 
	      return EXIT_WRONG_TYPE ;
	    }
	    @header = @arr;
# 	    shift @header;shift @header;$extra_arr += 2;  ## remove from header date and time
	    $dbh->add_new_columns ($table_name, \@header);
	} else {
	    ## no support for new/changed fields in middle of the line
	    if ( scalar @arr + $extra_arr != scalar @header || $arr[0] !~ m/[0-9]{2}\/[0-9]{2}\/[0-9]{2}/ ){
		DEBUG "filename = $filename\n\trow=$count\n\tarr=".(scalar @arr)."\n\textra=$extra_arr\n\theader_arr=".(scalar @header)."\n\tfirst=$arr[0]\n\tsecond=$arr[1])\n";
		LOGDIE "Strange line nr $count\n\t$line\n\tfrom file $filename\n" if $arr[0] ne "Date" || scalar @arr + $extra_arr != scalar @header;
		next;
	    }

	    my ($d, $m, $y) = split '/', $arr[0]; #21/07/12
	    my ($h, $min, $s) = split ':', $arr[1]; #03:00:15
	    ## timelocal doesn't take GMT difference in consideration, rrd graph the same. this will insert a wrong time that will be corrected by rrd
	    my $timestamp = timelocal($s, $min, $h, $d, $m-1, $y+2000); ## or timegm
	    unshift @arr, $timestamp; ## put timestamp in front
	    if ($second) {  ## fix header : values are like 0:0:0:0 => split in 0,0,0,0
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
	      $second = 0;
	    }
	    @arr = map{split /;/} @arr;
	    my @crt_vals;
	    ## 1=timestamp, 2=date, 3=time
	    push @crt_vals, ($data->{id}, $data->{host_id}, $arr[0], $dbh->getQuotedString($arr[1]), $dbh->getQuotedString($arr[2])) ;
	    for (my $i = 3; $i < scalar @header; $i++) {
# 		$columns .= ",$header_hash->{$header[$i]}" if $count == 2;
# 		$new_vals .= ", $arr[$i]";
		push @columns, $header_hash->{$header[$i]} if $count == 2;
		push @crt_vals, $arr[$i];
	    }
# 	    push @values, " ($data->{id}, ".$data->{host_id}.", $arr[0], '$arr[1]', '$arr[2]' $new_vals) ";
	    push @values, \@crt_vals;
	    my $e=tv_interval($t0);$t2 += $e;$t1 += $e;
	    if ($count % 200 == 0){
		DEBUG "Insert ".(scalar @values)." rows took $t2 ($name$suffix)\n";$t2=0;
# 		$dbh->insertRowsDB($table_name, "(file_id, host_id, $header[0], date, time $columns)", join ",", @values) if scalar @values;
		$dbh->insertRowsDB($table_name, ['file_id', 'host_id', $header[0], 'date', 'time', @columns], @values);
# 		addRows($dbh, $table_name, ['file_id', 'host_id', $header[0], 'date', 'time', @columns], @values);
		@values = ();
	    };
	    $t0 = [gettimeofday];;
	}
    }
    close (MYFILE);
#     $dbh->insertRowsDB($table_name, "(file_id, host_id, $header[0], date, time $columns)", join ",", @values) if scalar @values;
    TRACE "Insert ".(scalar @values)." rows took $t2 ($name$suffix)\n";$t2=0;
    $dbh->insertRowsDB($table_name, ['file_id', 'host_id', $header[0], 'date', 'time', @columns], @values);
#     addRows($dbh, $table_name, ['file_id', 'host_id', $header[0], 'date', 'time', @columns], @values);
    $dbh->updateFileColumns($data->{id}, ['parse_duration','parse_done_time'], [$dbh->getQuotedString($t1), 'NOW()']);
    DEBUG "Total parse for $name$suffix took $t1\n";
    INFO "Done stats for file $name$suffix id $data->{id}\n";
    return START_MUNIN;
}

return 1;
