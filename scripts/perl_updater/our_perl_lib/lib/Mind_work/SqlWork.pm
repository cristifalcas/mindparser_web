package SqlWork;

use warnings;
use strict;
$| = 1;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use DBI;
use Log::Log4perl qw(:easy);
use Mind_work::MindCommons;

my $db_h;
my $config = MindCommons::xmlfile_to_hash("config.xml");

use Definitions ':all';

sub new {
    my $class = shift;
    my $self = { };
    $db_h = DBI->connect("DBI:mysql:$config->{db_config}->{db_database}:10.0.0.99:3306", $config->{db_config}->{db_user}, $config->{db_config}->{db_pass},
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
    my $sth = $db_h->do("update $config->{db_config}->{collected_file_table} set status=$status WHERE id=$fileid") || die "Error $DBI::errstr\n";
}

sub updateFileInfo {
    my ($self, $fileid, $duration, $status) = @_;
    DEBUG "Updating in db file id=$fileid\n";
    if (defined $duration) {
	my $sth = $db_h->do("update $config->{db_config}->{collected_file_table} set 
	      parse_duration=$duration,
	      parse_done_time=NOW()
	  WHERE id=$fileid") || die "Error $DBI::errstr\n";
    } else {
	my $sth = $db_h->do("update $config->{db_config}->{collected_file_table} set 
	      status=$status WHERE id=$fileid") || die "Error $DBI::errstr\n";
    }
}

sub createStatsTable {
    my ($self, $table_name) = @_;
    my $query = $db_h->do("CREATE TABLE IF NOT EXISTS $table_name LIKE $config->{db_config}->{stats_template_table}") || die "Error $DBI::errstr\n";
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
    my $hash_ref = $db_h->selectrow_hashref("SELECT * FROM $config->{db_config}->{collected_file_table} WHERE id=$id");
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
    my $query = $db_h->prepare("select * from $config->{db_config}->{cust_table}");
    $query->execute() || die "Error $DBI::errstr\n";
    while (my @row = $query->fetchrow_array ){
	die "lame 1\n".Dumper(@row) if @row != 2;
	my $query_h = $db_h->prepare("select * from $config->{db_config}->{host_table} where customer_id=$row[0]");
	$query_h->execute() || die "Error $DBI::errstr\n";
	while (my @row_h = $query_h->fetchrow_array ){
	    die "lame 2\n".Dumper(@row_h) if @row_h != 4;
	    $hash->{$row[1]}->{'id'} = $row[0];
	    $hash->{$row[1]}->{'hosts'}->{$row_h[2]} = $row_h[0];
	}
    }
    return $hash;
}

sub fixMissingRRDs {
    my ($self, $status, $all_customers) = @_;
    DEBUG "Fixing missing RRDs\n";
    foreach my $cust_name (keys %$all_customers){
	my $cust_hosts = $all_customers->{$cust_name}->{hosts};
	foreach my $host_name (keys %$cust_hosts){
	    next if defined $db_h->selectrow_arrayref("select * from $config->{db_config}->{collected_file_table} where status=0 and host_id=$cust_hosts->{$host_name}");
	    my $rrd_files_nr = scalar @{ [glob("$Munin::Common::Defaults::MUNIN_DBDIR/$cust_name/$host_name.$cust_name-*")] };
	    ## no rrd files: set all files for this host to status $status, so we will create everything from zero
	    if (! $rrd_files_nr) {
		my $sth = $db_h->do("update $config->{db_config}->{collected_file_table} set status=$status WHERE host_id=$cust_hosts->{$host_name} and (status>$status and status<100)");
		DEBUG "Updated all files ($sth) from host=$host_name with status $status because of no rrd files\n" if ($sth ne "0E0");
	    }
	}
    }
}

sub timeFromLastUpdate {
    my ($self, $status, $h_id) = @_;
    DEBUG "Check if enough time has past from last file inserted\n";
    my ($last_time) = @{ $db_h->selectrow_arrayref("select unix_timestamp(max(parse_done_time)) from $config->{db_config}->{collected_file_table} where status=$status and host_id=$h_id") };
    return 0 if ! defined $last_time; ## broken munin update
    return (timegm(gmtime) - $last_time > 3) ? 1 : 0;
}

sub cleanDeletedHosts {
    my $config = MindCommons::xmlfile_to_hash("config.xml");
    use Cwd 'abs_path';
    use File::Basename;
    use Mind_work::MuninWork;

    DEBUG "Clean all files from missing hosts\n";
    my $script_path = (fileparse(abs_path($0), qr/\.[^.]*/))[1]."";
    my $res = $db_h->selectall_arrayref("select c.name cust,h.name from customers c,hosts h where h.customer_id=c.id");
    my $customers;
    $customers->{@$_[0]}->{@$_[1]} = 1 foreach (@$res);
    $customers->{localdomain}->{localhost} = 1;

# rrd $Munin::Common::Defaults::MUNIN_DBDIR/$CUST/$HOST.$CUST-$PLUGIN
    foreach my $file (glob ("$Munin::Common::Defaults::MUNIN_DBDIR/*/*.rrd")){
	my ($cust, $host, $cust2) = $file =~ /^$Munin::Common::Defaults::MUNIN_DBDIR\/(.*)\/(.*?)\.(.*?)-(.*).rrd$/;
	if (! defined $host || ! defined $cust){
	  DEBUG "Unknown file $file\n";
	  next;
	}
	DEBUG "Delete rrd $file\n" if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
	unlink $file if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
    }
# $Munin::Common::Defaults::MUNIN_SPOOLDIR/faker/$HOST.$CUST/datafile.$PLUGIN
    foreach my $file (glob ("$Munin::Common::Defaults::MUNIN_SPOOLDIR/faker/*/datafile.*")){
	my ($cust, $host, $plugin) = $file =~ /^$Munin::Common::Defaults::MUNIN_SPOOLDIR\/faker\/(.*)\.(.*?)\/datafile\.(.*)$/;
	if (! defined $host || ! defined $cust){
	  DEBUG "Unknown file $file\n";
	  next;
	}
	DEBUG "Delete datafile $file\n" if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
	unlink $file if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
    }
# $Munin::Common::Defaults::MUNIN_CONFDIR/munin-conf.d/$HOST.$CUST
    foreach my $file (glob ("$Munin::Common::Defaults::MUNIN_CONFDIR/munin-conf.d/*")){
	my ($host, $cust) = $file =~ /^$Munin::Common::Defaults::MUNIN_CONFDIR\/munin-conf.d\/(.*)\.(.*?)$/;
	if (! defined $host || ! defined $cust){
	  DEBUG "Unknown file $file\n";
	  next;
	}
	DEBUG "Delete config $file\n" if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
	unlink $file if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
    }
# $config->{dir_paths}->{uploads_dir}/$CUST/$HOST/
#     foreach my $file (glob ("$config->{dir_paths}->{uploads_dir}/*/*/*")){
# 	$config->{dir_paths}->{uploads_dir} =~ s/\/+/\//g;
# 	my ($cust, $host, $q) = $file =~ /^$config->{dir_paths}->{uploads_dir}\/(.*?)\/(.*?)\/(.*)$/;
# 	if (! defined $host || ! defined $cust){
# 	  DEBUG "Unknown file $file\n";
# 	  next;
# 	}
# 	if (! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host}) {
# 	    DEBUG "Delete uploaded $file\n";
# 	    unlink $file;
# 	}
#     }
# $config->{dir_paths}->{filedone_dir}/$CUST/$HOST/
# $config->{dir_paths}->{fileerr_dir}/error/$CUST/$HOST/
# $script_path/$config->{dir_paths}->{plugins_conf_dir_postfix}/customers/$CUST/$HOST/$PLUGIN.conf
}
my $time_last_update = time;
sub updateDatafile {
    use Mind_work::MuninWork;
    if (time - $time_last_update > 5){
	DEBUG "Update global datafile\n";
	cleanDeletedHosts;
	$time_last_update = time;
# 	MuninWork::writedatafile;
	my @all_lines;
	foreach (MindCommons::find_files_recursively("$Munin::Common::Defaults::MUNIN_SPOOLDIR/faker/")){
	    next if $_ !~ m/datafile/;
	    open(FILE, "$_") || LOGDIE "can't open file $_: $!\n";
	    my @new_lines = <FILE>;
	    push @all_lines, @new_lines;
	    close FILE;
	}
	open(FILE, ">$Munin::Common::Defaults::MUNIN_DBDIR/datafile") || LOGDIE "can't open file $_: $!\n";
	print FILE join "", @all_lines;
	close FILE;
	return if -s "$Munin::Common::Defaults::MUNIN_DBDIR/datafile" < 50;
#         DEBUG "Running global munin update\n";
	system("/opt/munin/lib/munin-update", "--nofork") == 0 or WARN "Error running global munin-update\n";
#         DEBUG "Running global munin limits\n";
	system("/opt/munin/lib/munin-limits") == 0 or WARN "Error running global munin-limits\n";
#         DEBUG "Running global munin html\n";
	system("/opt/munin/lib/munin-html", "--nofork") == 0 or WARN "Error running global munin-html\n";
    }
}

# start work if stats file done and no other stats pending or no rrd files or files exist for processing. thread should be per host+plugin
sub getWorkForMunin {
    my ($self, $status) = @_;
return;
    DEBUG "See if we have any work for munin\n";
    updateDatafile;
    my $all_customers = getCustomers();
    fixMissingRRDs($self, $status, $all_customers);

    use Time::Local;
    my $hash = $db_h->selectall_hashref("select * from $config->{db_config}->{collected_file_table} where status=$status", ['customer_id', 'host_id', 'inserted_in_tablename', 'status', 'id']); # and inserted_in_tablename is not null

    my $ret;
    foreach my $cust (keys %$hash){
	my $all_hosts = $hash->{$cust};
	my $cust_name = $self->get_customer_name($cust);
	foreach my $h_id (keys %$all_hosts){
	    ## we have files not processed yet
	    my $host_name = $self->get_host_name($h_id);
	    last if defined $db_h->selectrow_arrayref("select * from $config->{db_config}->{collected_file_table} where status=0 or status=5 and host_id=$h_id");
	    DEBUG "No more files are waiting for $host_name\n";
	    next if ! timeFromLastUpdate($self, $status, $h_id);
	    ## host name not defined: was deleted. set all files as not collected, so the stats thread will updated them with error. clean dirs
	    if (! defined $host_name) {
		DEBUG "Updating all files from hostid=$h_id with status EXIT_HOST_DELETE\n";
		my $sth = $db_h->do("update $config->{db_config}->{collected_file_table} set status=".EXIT_HOST_DELETE." WHERE host_id=$h_id") || die "Error $DBI::errstr\n";
		next;
	    }
	    my $host_tables = $all_hosts->{$h_id};
	    foreach my $table (keys %$host_tables){
		my $files_id = $host_tables->{$table}->{$status};
		TRACE "Adding hostid=$h_id for munin work\n";
		$ret->{$h_id."+".$table} = [keys %$files_id];
	    }
	}
    }
    return $ret;
}
## per worker_type+host_id, to speed file detection (1.8s vs 0.4)
sub getWorkForExtract {
    my ($self, $status) = @_;
    my $hash = $db_h->selectall_hashref("select * from $config->{db_config}->{collected_file_table} where status=$status limit 100", 'id');
    my $dir_hash;
    foreach my $id (keys %$hash) {
	$dir_hash->{$hash->{$id}->{worker_type}."_".$hash->{$id}->{host_id}}->{$id} = $hash->{$id};
    }
    return $dir_hash;
}

sub getFilesForParsers {
    my ($self, $status) = @_;
    my $hash = $db_h->selectall_hashref("select * from $config->{db_config}->{collected_file_table} where status=$status and worker_type='statistics'", 'id');
    return $hash;
}

sub insertFile {
    my ($self, $status, $file_hash) = @_;
    TRACE "Add to db file $file_hash->{file_info}->{name} with status $status.\n";
    my $cust_id = get_customer_id($file_hash->{machine}->{customer}) || EXIT_STATUS_NA;
    my $host_id = get_host_id($file_hash->{machine}->{host}, $cust_id) || EXIT_STATUS_NA;
    my $app = $file_hash->{worker}->{app} || EXIT_STATUS_NA;
    my $table_name = EXIT_STATUS_NA;
    if (defined $file_hash->{worker}->{table_name} && defined $file_hash->{machine}->{customer}) {
	$table_name = substr($file_hash->{worker}->{table_name}."_".lc($file_hash->{machine}->{customer}), 0, 64);
    }
    my $type = $file_hash->{worker}->{type} || EXIT_STATUS_NA;
    
    my $query = "INSERT INTO $config->{db_config}->{collected_file_table} 
	    (customer_id,
	     host_id,
	     file_name,
	     file_md5,
	     size,
	     inserted_in_tablename,
	     app_name,
	     worker_type,
	     status)
	VALUES (
	    $cust_id, 
	    $host_id, 
	    ".$db_h->quote($file_hash->{file_info}->{name}).", 
	    ".$db_h->quote($file_hash->{file_info}->{md5}).", 
	    $file_hash->{file_info}->{size},
	    ".$db_h->quote($table_name).", 
	    ".$db_h->quote($app).", 
	    ".$db_h->quote($type).", 
	    $status)";
    my $query_handle = $db_h->prepare($query);
    $query_handle->execute() || die "Error $DBI::errstr\n" || die "Error $DBI::errstr\n";
    my $in_id = $db_h->{ q{mysql_insertid}};
    return $in_id;
}

sub get_customer_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select name from $config->{db_config}->{cust_table} where id=$id");
    return $arr_ref->[0];
}

sub get_customer_id {
    my $name = shift;
    my $arr_ref = $db_h->selectrow_arrayref("select id from $config->{db_config}->{cust_table} where name=".$db_h->quote($name));
    return $arr_ref->[0];
}

sub get_host_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select name from $config->{db_config}->{host_table} where id=$id");
    return $arr_ref->[0];
}

sub get_host_id {
    my ($name, $cust_id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select id from $config->{db_config}->{host_table} where customer_id=$cust_id and name=".$db_h->quote($name));
    return $arr_ref->[0];
}

sub get_file_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select file_name from $config->{db_config}->{collected_file_table} where id=$id");
    return $arr_ref->[0];
}

sub get_md5_names {
    my ($self, @arr) = @_;
    my $existing = $db_h->selectall_hashref("select * from $config->{db_config}->{md5_names_table}", 'name');
#     my $sha1_hash;
#     $sha1_hash->{$_} = "x_".MindCommons::get_string_sha($_) foreach (@arr);

    my $res;
    foreach (@arr){
	next if $_ eq 'id' || $_ eq 'file_id' || $_ eq 'timestamp';
	die "ce ma fac cu $_?\n" if ! defined $existing->{$_};
# 	$db_h->do("INSERT IGNORE INTO $config->{db_config}->{md5_names_table} (md5, name) VALUES ('$sha1_hash->{$_}', '$_')") || die "Error $DBI::errstr\n";
	$res->{$_} = $existing->{$_}->{md5};
    }
    return $res;
} 

sub add_new_columns {
    my ($self, $table_name, $arr) = @_;
    DEBUG "Acquiring lock\n";
    $db_h->do("LOCK TABLES $table_name WRITE, $config->{db_config}->{md5_names_table} WRITE");
    my $sha1_hash;
    $sha1_hash->{"x_".MindCommons::get_string_sha($_)} = $_ foreach (@$arr);
    my $md5_existing = $db_h->selectall_hashref("select * from $config->{db_config}->{md5_names_table}", 'md5');
    foreach (keys %$sha1_hash){
	next if defined $md5_existing->{$_};
	INFO "Adding new rows $sha1_hash->{$_} ($_) in $config->{db_config}->{md5_names_table}\n";
	$db_h->do("INSERT IGNORE INTO $config->{db_config}->{md5_names_table} (md5, name) VALUES ('$_', '$sha1_hash->{$_}')");
    }
    my $columns_e = getColumnList($self, $table_name);
    my $md5_columns = ['id', 'file_id', 'timestamp', keys %$sha1_hash];
    my ($only_in_arr1, $only_in_arr2, $intersection) = MindCommons::array_diff( $md5_columns, $columns_e);

    while (my ($index, $sha1) = each @$only_in_arr1) {
	INFO "Adding new column $sha1 ($sha1_hash->{$sha1}) for table $table_name\n";
	$db_h->do("ALTER TABLE $table_name ADD $sha1 decimal(15,5)") || die "Error $DBI::errstr\n";
# 	$db_h->do("INSERT IGNORE INTO $config->{db_config}->{md5_names_table} (md5, name) VALUES ('$sha1', '$arr->[$index]')") || die "Error $DBI::errstr\n";
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
