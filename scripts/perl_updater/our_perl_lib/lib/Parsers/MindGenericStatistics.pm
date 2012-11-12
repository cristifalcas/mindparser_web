package MindGenericStatistics;

use warnings;
use strict;
$| = 1;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Time::HiRes qw( usleep tv_interval gettimeofday);
use Time::Local 'timelocal', 'timelocal_nocheck';;
use Definitions ':all';
use Log::Log4perl qw(:easy);

my $plugin_info;
my $max_rows = 5000;
my $hash_vals;
my $t0 = [gettimeofday];my $t1=0;my $t2=0;

sub new {
    my $class = shift;
    my $self = { };

    bless($self, $class);
    return $self;
}

sub getArray {
    my ($line, $delim) = @_;
    $delim = "," if not defined $delim;
    chomp $line;
    my @arr = map {s/(\r)|(^\s*)|(\s*$)//g; $_ } split /$delim/, $line;
    return @arr;
}

sub extractHeader {
    my @arr = @_;
    DEBUG "Found new header.\n";
    ## first is date, second is time
    shift @arr;shift @arr;
    my ($header, $groupby_index);
    my $name = $stats_default_info->{$plugin_info->{plugin_name}}->{group_by}->[0];
#     LOGDIE "Plugin $plugin_info->{plugin_name} needs to be configured.\n" if ! defined $name;
    while (my ($index, $val) = each @arr) {
	$groupby_index->{$index} = $val if (defined $name && $val =~ m/^$name$/i);
	push @$header, $val;
	LOGDIE "We have this hardcoded: $val.\n" if $val eq 'id' || $val eq 'file_id' || $val eq 'host_id' || $val eq 'timestamp' || $val eq 'group_by';
    }
    return ($header, $groupby_index);
}

sub extractValues {
    my @arr = @_;
    my ($values, $multi_values);
    ## first is date, second is time
    my ($d, $m, $y) = split '/', $arr[0]; #21/07/12
    my ($h, $min, $s) = split ':', $arr[1]; #03:00:15
    shift @arr;shift @arr;
    ## timelocal doesn't take GMT difference in consideration, rrd graph the same. this will insert a wrong time that will be corrected by rrd
    my $timestamp = timelocal($s, $min, $h, $d, $m-1, $y+2000); ## or timegm

    while (my ($index, $val) = each @arr) {
	if ($val =~ m/;/) {
	    $multi_values->{$index} = $val;
	} else {
	    push @$values, $val;
	}
    }
    return ($timestamp, $values, $multi_values);
}

sub fixHeader {
    my ($header, $multi_values, $groupby_index) = @_;
    ## remove group by and replace multi values
    foreach my $index (sort keys %$multi_values) {
	my $i = 0;
	my @q = map {$header->[$index]."_".$i++} split /;/, $multi_values->{$index};
	splice(@$header, $index, 1, @q); 
    }
    foreach my $index (sort keys %$groupby_index){
	splice(@$header, $index, 1); 
    }
    return $header;
}

sub fixValues {
    my ($values, $multi_values, $groupby_index) = @_;
    my $group_by = "";
    # remove group by and replace multi value
    foreach my $index (sort keys %$multi_values) {
	splice(@$values, $index, 1, split /;/, $multi_values->{$index}); 
    }
    LOGDIE "We don't know what to do with multiple group bys.".Dumper($groupby_index) if scalar keys %$groupby_index > 1;
    foreach my $index (sort keys %$groupby_index){
	$group_by = "$groupby_index->{$index}_$values->[$index]";
	splice(@$values, $index, 1); 
    }
    return ($values, $group_by);
}

sub insertRows {
    my ($header, $table_name, $data, $dbh) = @_;
    $dbh->add_new_columns ($table_name, $header);
    my @values;
    foreach my $group (keys %$hash_vals) {
	my $time_vals = $hash_vals->{$group};
	foreach my $times (keys %$time_vals) {
	    push @values, [$data->{id}, $data->{host_id}, $times, $dbh->getQuotedString($group), @{$time_vals->{$times}}];
	}
    }

    my $columns = ['file_id', 'host_id', 'timestamp', 'group_by'];
    my $header_hash = $dbh->get_md5_names($header);
    push $columns, $header_hash->{$_} foreach (@$header) ;
    $dbh->insertRowsTable($table_name, $columns, @values);
    undef $hash_vals;
    INFO "Insert ".(scalar @values)." rows took $t2\n";$t2=0;
}

sub parse {
    my ($self, $data, $dbh) = @_;
    $plugin_info = $data->{plugin_info};

    my $filename = $data->{file_name};
    my $table_name = $data->{plugin_info}->{inserted_in_tablename};
    INFO "Parsing file $filename (id=$data->{id}) as $table_name from cust=$data->{customer_name}, host=$data->{host_name}.\n";

    $dbh->createStatsTable($table_name);
    my ($header, $values, $groupby_index, $multi_values, $is_new_header, $timestamp);
    my $count = 0;

    open (MYFILE, "$filename") or LOGDIE  "Couldn't open $filename: $!";
    while (<MYFILE>) {
	my $line = $_;
	next if $line =~ m/^\s*$/;
	my $e=tv_interval($t0);$t2 += $e;$t1 += $e;
	my @line_arr = getArray($line);
	$count++;
	if ($line =~ m/^Date\s*,\s*Time\s*,/i){
	    insertRows($header, $table_name, $data, $dbh) if defined $header;
	    ($header, $groupby_index) = extractHeader(@line_arr);
	    $is_new_header = 1;
	} else {
	    if (! defined $header) {
		INFO "Wrong line: \n\t$line\n".Dumper($header);
		return EXIT_WRONG_TYPE ;
	    }
	    ($timestamp, $values, $multi_values) = extractValues(@line_arr);
	    $header = fixHeader($header, $multi_values, $groupby_index) if $is_new_header;
	    ($values, my $group_by) = fixValues($values, $multi_values, $groupby_index);
	    LOGDIE "Strange line (header<>values):".Dumper($header, $values) if scalar @$header != scalar @$values;
	    $hash_vals->{$group_by}->{$timestamp} = $values;
	    $is_new_header = 0;
	    insertRows($header, $table_name, $data, $dbh) if ($count % $max_rows == 0);
	}
	$t0 = [gettimeofday];
    }
    close (MYFILE);
    insertRows($header, $table_name, $data, $dbh);
    $dbh->updateFileColumns($data->{id}, ['parse_duration','parse_done_time'], [$dbh->getQuotedString($t1), 'NOW()']);
    INFO "Done stats for file $filename in $t1 ms.\n";
    return START_MUNIN;
}

return 1; 
