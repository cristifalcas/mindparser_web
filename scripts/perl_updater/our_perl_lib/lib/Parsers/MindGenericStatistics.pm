package MindGenericStatistics;

use warnings;
use strict;
$| = 1;

use Data::Dumper;
# $Data::Dumper::Sortkeys = 1;
use Time::HiRes qw( usleep tv_interval gettimeofday);
use Time::Local 'timelocal', 'timelocal_nocheck';;
use Definitions ':all';
use Log::Log4perl qw(:easy);

my $plugin_info;
my $max_rows = 5000;
my ($t0, $t1, $t2);
my ($dbh, $data, $fucker_line, $fucker_header);

sub new {
    my $class = shift;
    my $self = { db_conn => shift, data => shift };

    $dbh = $self->{db_conn};
    $data = $self->{data};
    bless($self, $class);
    return $self;
}

sub getArray {
    my ($line, $delim) = @_;
    $line =~ s/\R//g;
    my @arr = split /$delim/, $line;
    return @arr;
}

sub extractHeader {
    my @arr = @_;
    DEBUG "Found new header.\n";
    ## first is date, second is time
    shift @arr;shift @arr;
    my ($header, $groupby_index);
    my $group_by_name = $stats_default_info->{$plugin_info->{plugin_name}}->{group_by}->[0];

    while (my ($index, $val) = each @arr) {
	LOGDIE "We have this hardcoded: $val.\n" if $val eq 'id' || $val eq 'file_id' || $val eq 'host_id' || $val eq 'timestamp' || $val eq 'group_by';
	$groupby_index->{$index} = $val if (defined $group_by_name && $val =~ m/^$group_by_name$/i);
	push @$header, $val;
    }

    return ($header, $groupby_index);
}

sub extractValues {
    my @arr = @_;
    my ($d, $m, $y) = split '/', $arr[0]; #21/07/12
    my ($h, $min, $s) = split ':', $arr[1]; #03:00:15
    ## timelocal doesn't take GMT difference in consideration, rrd graph the same. this will insert a wrong time that will be "corrected" by rrd
    my $timestamp = timelocal($s, $min, $h, $d, $m-1, $y+2000);

    ## first is date, second is time
    shift @arr;shift @arr;
    my ($values, $multi_values);
    my $split_multi = $stats_default_info->{$plugin_info->{plugin_name}}->{multi_value}->[0];
    while (my ($index, $val) = each @arr) {
	## for ex dbques contains ;
	$multi_values->{$index} =  [split /$split_multi/, $val] if (defined $split_multi && $val =~ m/$split_multi/);
	push @$values, $val;
    }
    return ($timestamp, $values, $multi_values);
}

sub fixHeader {
    my ($header, $multi_values, $groupby_index) = @_;

    my $hash;
    while (my ($index, $elem) = each @$header) {
      $hash->{$index} = $elem;
    }
    ## remove group by
    foreach my $index (sort keys %$groupby_index){
	delete $hash->{$index};
    }
    ## replace multi values
    foreach my $index (sort keys %$multi_values) {
	my $i = 0;
	$hash->{$index} = [map {$header->[$index]."_".$i++} @{$multi_values->{$index}}];
    }

    $header = [];
    foreach my $index (sort {$a <=> $b} keys %$hash) {
	if (ref($hash->{$index}) eq "ARRAY") {
	    push @$header, @{ $hash->{$index} };
	} else {
	    push @$header, $hash->{$index};
	}
    }
    return $header;
}

sub fixValues {
    my ($values, $multi_values, $groupby_index) = @_;
    LOGDIE "We don't know what to do with multiple group bys.".Dumper($groupby_index) if scalar keys %$groupby_index > 1;
    my $group_by = "";

    my $hash;
    while (my ($index, $elem) = each @$values) {
      $hash->{$index} = $elem;
    }
    ## remove group by
    foreach my $index (sort keys %$groupby_index){
	delete $hash->{$index};
	$group_by = "$groupby_index->{$index}_$values->[$index]";
    }
    ## replace multi values
    foreach my $index (sort keys %$multi_values) {
	$hash->{$index} = $multi_values->{$index};
    }

    $values = [];
    foreach my $index (sort {$a <=> $b} keys %$hash) {
	if (ref($hash->{$index}) eq "ARRAY") {
	    push @$values, @{ $hash->{$index} };
	} else {
	    push @$values, $hash->{$index};
	}
    }
    return ($values, $group_by);
}

sub insertRows {
    my ($header, $hash_vals, $table_name) = @_;
    $dbh->add_new_columns($table_name, $header);
    $dbh->setPluginDefaults($data->{plugin_id});
    my @values;
    foreach my $group (keys %$hash_vals) {
	my $time_vals = $hash_vals->{$group};
	foreach my $times (keys %$time_vals) {
	    push @values, [$data->{id}, $data->{host_id}, $times, $dbh->getQuotedString($group), @{$time_vals->{$times}}];
	    undef $hash_vals->{$group}->{$times};
	}
	undef $hash_vals->{$group};
    }

    my $columns = [@$columns_header];
    my $header_hash = $dbh->get_md5_names($header);
    foreach (@$header) {
	push $columns, $header_hash->{$_};
    }

    $dbh->insertRowsTable($table_name, $columns, @values);
    undef $hash_vals;
#     INFO "Insert ".(scalar @values)." rows took ".tv_interval($t1)."\n";$t1=[gettimeofday];
}

sub isHeader {
    my ($str) = @_;
    return $str =~ m/^Date\s*,\s*Time\s*,/i;
}

sub updateError {
    my ($line_nr, $str) = @_;
    ERROR "Got error: $str.\n";
    my $err = $fucker_header;
    chomp $fucker_line;
    $fucker_header = "";
    $err .= "$fucker_line\t => at line nr $line_nr we got: $str\n";
    return $err;
}

sub parse {
    my ($self) = @_;
    $plugin_info = $data->{plugin_info};

    my $filename = $data->{file_name};
    my $table_name = $data->{plugin_info}->{inserted_in_tablename};
    INFO "Parsing file $filename (id=$data->{id}) as $table_name from cust=$data->{customer_name}, host=$data->{host_name}.\n";
    $dbh->createStatsTable($table_name);

    my ($header, $groupby_index, $is_new_header, $hash_vals, $header_nr_elem, $value_nr_elem);
    my $delim = $stats_default_info->{$plugin_info->{plugin_name}}->{delim};
    $delim = "," if ! defined $delim;
    my $errors = "";
    my $count = 0; my $nr_lines = 0;

$t0 = [gettimeofday];$t1=[gettimeofday];$t2=[gettimeofday];
    open (MYFILE, $filename) or LOGDIE  "Couldn't open $filename: $!";
    while (<MYFILE>) {
if ($nr_lines % $max_rows == 0){INFO "between $max_rows lines took : ".tv_interval($t2)."\n";$t2=[gettimeofday];};
	my $line = $_;
	$nr_lines++;
$fucker_line = $line;
	next if $line =~ m/^\s*$/;
	my @line_arr = getArray($line, $delim);
	$count++;
	if (isHeader($line)){
$fucker_header = $line;
	    $header_nr_elem = scalar @line_arr;
	    insertRows($header, $hash_vals, $table_name) if defined $header;
	    ($header, $groupby_index) = extractHeader(@line_arr);
	    $is_new_header = 1;
	} else {
	    if (! defined $header) {
		$errors .= updateError($nr_lines, "No header set.");
		next;
	    }
	    if ($header_nr_elem != scalar @line_arr) {
		$errors .= updateError($nr_lines, "header and values don't match.");
		next;
	    }
	    my ($timestamp, $values, $multi_values) = extractValues(@line_arr);
	    $header = fixHeader($header, $multi_values, $groupby_index) if $is_new_header;
	    $is_new_header = 0;
	    ($values, my $group_by) = fixValues($values, $multi_values, $groupby_index);
	    LOGDIE "Strange line (header<>values) after fix:".Dumper($header, $values) if scalar @$header != scalar @$values;
	    $hash_vals->{$group_by}->{$timestamp} = $values;
	    insertRows($header, $hash_vals, $table_name) if ($count % $max_rows == 0);
	}
    }
    close (MYFILE);
    insertRows($header, $hash_vals, $table_name);
    $dbh->updateFileColumns($data->{id}, ['parse_duration','parse_done_time'], [$dbh->getQuotedString($t0), 'NOW()']);
    INFO "Done stats for file $filename in ".tv_interval($t0)." ms.\n";
LOGDIE Dumper($errors);
    return START_MUNIN;
}

return 1; 
