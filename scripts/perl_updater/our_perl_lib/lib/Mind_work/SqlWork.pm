package SqlWork;

use warnings;
use strict;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use DBI;

use Mind_work::MindCommons;
my ($db_database, $db_user, $db_pass, 
  $cust_table, $host_table, $collected_file_table, $md5_names_table, $stats_template_table, 
  $db_h);

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({ level   => $INFO,
#                            file    => ">>test.log" 
			   layout   => "%d [%5p] (%6P) %m%n",
});

use constant {
    EXIT_STATUS_NA	=>-1,
    EXIT_IGNORE		=> 0,
    EXIT_STATS_EXPECTS	=> 0,
    EXIT_STATS_SUCCESS	=> 1,
    EXIT_MUNIN_EXPECTS	=> 1,
    EXIT_MUNIN_SUCCESS	=> 2,
    EXIT_MUNIN_FINISH	=> 4,
    EXIT_PARSE_EXPECTS	=> 0,
    EXIT_PARSE_SUCCESS	=> 3,
    EXIT_NO_FILE	=> 100,
    EXIT_WRONG_TYPE	=> 102,
    EXIT_NO_LINES	=> 110,
    EXIT_WRONG_MINE	=> 122,
    EXIT_EXTR_ERR	=> 150,
    EXIT_NO_ROWS	=> 200,
    EXIT_NO_RRD		=> 210,
    EXIT_MAIN_ERROR	=> 1000,
    EXIT_MUNIN_ERROR	=> 130,
};

sub new {
    my $class = shift;
    my $config = MindCommons::xmlfile_to_hash("config.xml");
    $db_database = $config->{db_config}->{db_database};
    $db_user = $config->{db_config}->{db_user};
    $db_pass = $config->{db_config}->{db_pass};
    $cust_table = $config->{db_config}->{cust_table};
    $host_table = $config->{db_config}->{host_table};
    $collected_file_table = $config->{db_config}->{collected_file_table};
    $md5_names_table = $config->{db_config}->{md5_names_table};
    $stats_template_table = $config->{db_config}->{stats_template_table};
    
    my $self = { };
    $db_h = DBI->connect("DBI:mysql:$db_database", $db_user, $db_pass,
	{ ShowErrorStatement => 1,
          AutoCommit => 1,
          RaiseError => 1,
          mysql_use_result => 0,
          mysql_enable_utf8 => 1,
	  mysql_auto_reconnect => 1,
          PrintError => 1, }) || die "Could not connect to database: $DBI::errstr";
    bless($self, $class);
    return $self;
}

sub getDBI_handler {
    return $db_h;
}

sub updateFileStatus {
    my ($self, $fileid, $status) = @_;
    DEBUG "Updating file id=$fileid with status $status\n";
    my $sth = $db_h->do("update $collected_file_table set status=$status WHERE id=$fileid") || die "Error $DBI::errstr\n";
}

sub updateFileInfo {
    my ($self, $fileid, $duration, $to_table_name) = @_;
    DEBUG "Updating file id=$fileid with table_name=$to_table_name\n";
    my $sth = $db_h->do("update $collected_file_table set 
	      parse_duration=$duration,
	      parse_done_time=NOW(),
	      inserted_in_tablename='$to_table_name'
	  WHERE id=$fileid") || die "Error $DBI::errstr\n";
}

sub createStatsTable {
    my ($self, $table_name) = @_;
    my $query = $db_h->do("CREATE TABLE IF NOT EXISTS $table_name LIKE $stats_template_table") || die "Error $DBI::errstr\n";
    $db_h->do("ALTER TABLE $table_name add FOREIGN KEY (host_id) REFERENCES hosts(id)") || die "Error $DBI::errstr\n" if ! defined $query;
}

sub cloneForFork {
#     http://www.perlmonks.org/?node_id=594175
    my $self = shift;
    my $child_dbh = $db_h->clone();
    $db_h->{InactiveDestroy} = 1;
    undef $db_h;
    $db_h = $child_dbh;
}

sub getFile {
    my ($self, $id) = @_;
    my $hash_ref = $db_h->selectrow_hashref("SELECT * FROM $collected_file_table WHERE id=$id");
    return $hash_ref;
}

sub getColumnList {
  my($self, $table) = @_;
  
  my $sth = $db_h->prepare("SELECT * FROM $table WHERE 1=0");
  $sth->execute || die "Error $DBI::errstr\n";
  my $cols = $sth->{NAME}; # or NAME_lc if needed
  $sth->finish;
  return $cols;
}

sub getCustomers {
    my $self = shift;
    my $hash;
    my $query = $db_h->prepare("select * from $cust_table");
    $query->execute() || die "Error $DBI::errstr\n";
    while (my @row = $query->fetchrow_array ){
	die Dumper(@row)."lame 1\n" if @row != 2;
	my $query_h = $db_h->prepare("select * from $host_table where customer_id=$row[0]");
	$query_h->execute() || die "Error $DBI::errstr\n";
	while (my @row_h = $query_h->fetchrow_array ){
	    die Dumper(@row_h)."lame 2\n" if @row_h != 4;
	    $hash->{$row[1]}->{'id'} = $row[0];
	    $hash->{$row[1]}->{'hosts'}->{$row_h[2]} = $row_h[0];
	}
    }
    return $hash;
}

sub fixMissingRRDs {
    my ($self, $status, $all_customers) = @_;
    foreach my $cust_name (keys %$all_customers){
	my $cust_hosts = $all_customers->{$cust_name}->{hosts};
	foreach my $host_name (keys %$cust_hosts){
	    next if defined $db_h->selectrow_arrayref("select * from $collected_file_table where status=0 and host_id=$cust_hosts->{$host_name}");
	    my $rrd_files_nr = scalar @{ [glob("$Munin::Common::Defaults::MUNIN_DBDIR/$cust_name/$host_name.$cust_name-*")] };
	    ## no rrd files: set all files for this host to status $status, so we will create everything from zero
	    if (! $rrd_files_nr) {
		my $sth = $db_h->do("update $collected_file_table set status=$status WHERE host_id=$cust_hosts->{$host_name} and (status>$status and status<100)");
		DEBUG "Updated all files ($sth) from host=$host_name with status $status because of no rrd files\n" if ($sth ne "0E0");
	    }
	}
    }
}

sub timeFromLastUpdate {
    my ($self, $status, $h_id) = @_;
    my ($last_time) = @{ $db_h->selectrow_arrayref("select max(parse_done_time) from $collected_file_table where status=$status and host_id=$h_id") };
    LOGDIE "no parse_done_time for $collected_file_table\n" if ! defined $last_time;
    my ($date_f, $hour_f) = split " ", $last_time;
    my ($hour, $min, $sec) = split ":", $hour_f;
    my ($year, $mon, $day) = split "-", $date_f;
    my $unixtime = timelocal ($sec, $min, $hour, $day, $mon-1, $year);
# print Dumper($last_time, $unixtime,timegm(gmtime));
    return (timegm(gmtime) - $unixtime > 3) ? 1 : 0;
}

# start work if stats file done and no other stats pending or no rrd files or files exist for processing. thread should be per host
sub getWorkForMunin {
    my ($self, $status) = @_;
# print Dumper(getCustomers);
    my $all_customers = getCustomers();
    fixMissingRRDs($self, $status, $all_customers);

    use Time::Local;
    my $hash = $db_h->selectall_hashref("select * from $collected_file_table where status=$status", ['customer_id', 'host_id', 'inserted_in_tablename', 'status', 'id']); # and inserted_in_tablename is not null
    my $ret;
    foreach my $cust (keys %$hash){
	my $all_hosts = $hash->{$cust};
	my $cust_name = get_customer_name($self, $cust);
	foreach my $h_id (keys %$all_hosts){
	    ## we have files not processed yet
	    last if defined $db_h->selectrow_arrayref("select * from $collected_file_table where status=0 and host_id=$h_id");
	    return if ! timeFromLastUpdate($self, $status, $h_id);
	    my $host_name = get_host_name($self, $h_id);
	    ## host name not defined: was deleted. set all files as not collected, so the stats thread will updated them with error
	    if (! defined $host_name) {
		DEBUG "Updating all files from hostid=$h_id with status 0\n";
		my $sth = $db_h->do("update $collected_file_table set status=0 WHERE host_id=$h_id") || die "Error $DBI::errstr\n";
		return;
	    }
	    my $host_tables = $all_hosts->{$h_id};
	    foreach my $table (keys %$host_tables){
		my $files_id = $host_tables->{$table}->{$status};
		TRACE "Adding hostid=$h_id for munin work\n";
		$ret->{$h_id}->{$table} = [keys %$files_id];
	    }
	}
    }
    return $ret;
}

sub doneWorkForMunin {
    my ($self, $id, $new_status, $old_status) = @_;
    DEBUG "Done munin: Updating all files from hostid=$id with status $new_status\n";
    my $sth = $db_h->do("update $collected_file_table set status=$new_status WHERE status=$old_status and host_id=$id") || die "Error $DBI::errstr\n";
    return $new_status;
}

sub getFilesForParsers {
    my ($self) = @_;
    my $hash = $db_h->selectall_hashref("select * from $collected_file_table where status=0", 'id');
    return $hash;
}

sub insertFile() {
    my ($self, $customer_id, $host_id, $file_name, $file_size, $status) = @_;
    return if ! -f $file_name;
    my $md5 = MindCommons::get_file_sha($file_name);
    DEBUG "Inserting file $file_name with status $status.\n";
    my $query = "INSERT INTO $collected_file_table 
	    (customer_id,
	     host_id,
	     file_name,
	     file_md5,
	     size,
	     status)
	VALUES (
	    $customer_id, 
	    $host_id, 
	    ".$db_h->quote($file_name).", 
	    ".$db_h->quote($md5).", 
	    $file_size,
	    $status)";
    my $query_handle = $db_h->prepare($query);
    $query_handle->execute() || die "Error $DBI::errstr\n" || die "Error $DBI::errstr\n";
}

sub get_customer_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select * from customers where id=$id");
    return $arr_ref->[1];
}

sub get_host_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select * from hosts where id=$id");
    return $arr_ref->[2];
}

sub get_md5_names {
    my ($self, @arr) = @_;
    my $existing = $db_h->selectall_hashref("select * from $md5_names_table", 'name');
#     my $sha1_hash;
#     $sha1_hash->{$_} = "x_".MindCommons::get_string_sha($_) foreach (@arr);

    my $res;
    foreach (@arr){
	next if $_ eq 'id' || $_ eq 'file_id' || $_ eq 'timestamp';
	die "ce ma fac cu $_?\n" if ! defined $existing->{$_};
# 	$db_h->do("INSERT IGNORE INTO $md5_names_table (md5, name) VALUES ('$sha1_hash->{$_}', '$_')") || die "Error $DBI::errstr\n";
	$res->{$_} = $existing->{$_}->{md5};
    }
    return $res;
} 

sub add_new_columns {
    my ($self, $table_name, $arr) = @_;
    DEBUG "Acquiring lock\n";
    $db_h->do("LOCK TABLES $table_name WRITE, $md5_names_table WRITE");
    my $sha1_hash;
    $sha1_hash->{"x_".MindCommons::get_string_sha($_)} = $_ foreach (@$arr);
    my $md5_existing = $db_h->selectall_hashref("select * from $md5_names_table", 'md5');
    foreach (keys %$sha1_hash){
	next if defined $md5_existing->{$_};
	INFO "Adding new rows $sha1_hash->{$_} ($_) in $md5_names_table\n";
	$db_h->do("INSERT IGNORE INTO $md5_names_table (md5, name) VALUES ('$_', '$sha1_hash->{$_}')");
    }
    my $columns_e = getColumnList($self, $table_name);
    my $md5_columns = ['id', 'file_id', 'timestamp', keys %$sha1_hash];
    my ($only_in_arr1, $only_in_arr2, $intersection) = MindCommons::array_diff( $md5_columns, $columns_e);

    while (my ($index, $sha1) = each @$only_in_arr1) {
	INFO "Adding new column $sha1 ($sha1_hash->{$sha1}) for table $table_name\n";
	$db_h->do("ALTER TABLE $table_name ADD $sha1 decimal(15,5)") || die "Error $DBI::errstr\n";
# 	$db_h->do("INSERT IGNORE INTO $md5_names_table (md5, name) VALUES ('$sha1', '$arr->[$index]')") || die "Error $DBI::errstr\n";
    }
    DEBUG "Releasing lock\n";
    $db_h->do("UNLOCK TABLES");
}

sub insertRowsDB {
    my ($self, $table_name, $col, $val) = @_;
    $db_h->do("INSERT IGNORE INTO $table_name $col VALUES $val") || die "Error $DBI::errstr\n";
}

sub disconnect {
    $db_h->disconnect(); 
}

return 1;
