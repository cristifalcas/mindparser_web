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

sub insertFile {
    my ($self, $file_hash, $status) = @_;
    TRACE "Add to db file $file_hash->{file_info}->{name} with status $status.\n";
    my $cust_id = $self->get_customer_id($file_hash->{machine}->{customer}) || EXIT_STATUS_NA;
    my $host_id = $self->get_host_id($file_hash->{machine}->{host}, $cust_id) || EXIT_STATUS_NA;

    $db_h->do("INSERT INTO $config->{db_config}->{collected_file_table} 
	    (customer_id,
	     host_id,
	     plugin_id,
	     file_name,
	     file_md5,
	     size,
	     status)
	VALUES (
	    $cust_id, 
	    $host_id,".
	    EXIT_STATUS_NA.",
	    ".$db_h->quote($file_hash->{file_info}->{name}).", 
	    ".$db_h->quote($file_hash->{file_info}->{md5}).", 
	    $file_hash->{file_info}->{size},
	    $status)") || die "Error $DBI::errstr\n";
}

sub increasePluginQueue {
    my ($self, $plugin_id) = @_;
    $db_h->do("update $config->{db_config}->{plugins_table} 
		      set files_queue=IFNULL(files_queue, 0) + 1 WHERE id=$plugin_id")
	  || die "Error $DBI::errstr\n";
}

sub decreasePluginQueue {
    my ($self, $plugin_id) = @_;
    $db_h->do("update $config->{db_config}->{plugins_table} 
		      set files_queue=files_queue - 1 WHERE id=$plugin_id")
	  || die "Error $DBI::errstr\n";
}

sub nulifyPluginQueue {
    my ($self, $plugin_id) = @_;
    $db_h->do("update $config->{db_config}->{plugins_table} 
		      set files_queue=NULL WHERE id=$plugin_id")
	  || die "Error $DBI::errstr\n";
}

sub getPluginQueue {
    my ($self, $plugin_id) = @_;
    my $res = $db_h->selectrow_arrayref("select id from $config->{db_config}->{plugins_table} WHERE id=$plugin_id") || die "Error $DBI::errstr\n";
    return  $res->[0];
}

sub updateFileColumns {
    my ($self, $fileid, $columns, $values) = @_;

    LOGDIE "Wrong info received: ".Dumper($columns, $values) if scalar @$columns != scalar @$values;
    DEBUG "Updating file id=$fileid\n";
    my @txt;
    while (my ($index, $elem) = each @$columns) {
	TRACE "update file_id=$fileid: $columns->[$index]=$values->[$index]\n";
	push @txt, "$columns->[$index] = $values->[$index]";
    }
    $db_h->do("update $config->{db_config}->{collected_file_table} set ".(join ", ", @txt)." WHERE id=$fileid") || die "Error $DBI::errstr\n";
}

sub insertRowsTable {
    my ($self, $table_name, $cols, @vals) = @_;
    LOGDIE "We can't insert those: ".Dumper($table_name, $cols, @{ $vals[0] }) if scalar @$cols != scalar @{ $vals[0] };
    $_ = join ",", @$_ foreach @vals;
    $db_h->do("INSERT IGNORE INTO $table_name (".(join ",", @$cols).") VALUES (".(join ") , (", @vals).")") || die "Error $DBI::errstr\n";
}

sub createStatsTable {
    my ($self, $table_name) = @_;
    DEBUG "create new table $table_name\n";
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

sub getIDUsed {
    my ($self, $table_name, $cols, $vals) = @_;
    LOGDIE "Not good (cols nr <> vals nr)\n" if scalar @$cols != scalar @$vals;
    my @sel;
    push @sel, "$_=".(shift @$vals) foreach @$cols;
    my $res = $db_h->selectrow_arrayref("select id from $table_name where ".(join " and ", @sel)) || die "Error $DBI::errstr\n";
    return  $res->[0];
}

sub getQuotedString {
    my ($self, $string) = @_;
    return $db_h->quote($string);
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
	LOGDIE "lame 1\n".Dumper(@row) if @row != 2;
	my $query_h = $db_h->prepare("select * from $config->{db_config}->{host_table} where customer_id=$row[0]");
	$query_h->execute() || die "Error $DBI::errstr\n";
	while (my @row_h = $query_h->fetchrow_array ){
	    LOGDIE "lame 2\n".Dumper(@row_h) if @row_h != 4;
	    $hash->{$row[1]}->{'id'} = $row[0];
	    $hash->{$row[1]}->{'hosts'}->{$row_h[2]} = $row_h[0];
	}
    }
    return $hash;
}

# # # # sub fixMissingRRDs {
# # # #     my ($self, $status, $all_customers) = @_;
# # # #     DEBUG "Fixing missing RRDs\n";
# # # #     foreach my $cust_name (keys %$all_customers){
# # # # 	my $cust_hosts = $all_customers->{$cust_name}->{hosts};
# # # # 	foreach my $host_name (keys %$cust_hosts){
# # # # 	    next if defined $db_h->selectrow_arrayref("select * from $config->{db_config}->{collected_file_table} where status=0 and host_id=$cust_hosts->{$host_name}");
# # # # 	    my $rrd_files_nr = scalar @{ [glob("$Munin::Common::Defaults::MUNIN_DBDIR/$cust_name/$host_name.$cust_name-*")] };
# # # # 	    ## no rrd files: set all files for this host to status $status, so we will create everything from zero
# # # # 	    if (! $rrd_files_nr) {
# # # # 		my $sth = $db_h->do("update $config->{db_config}->{collected_file_table} set status=$status WHERE host_id=$cust_hosts->{$host_name} and (status>$status and status<100)");
# # # # 		DEBUG "Updated all files ($sth) from host=$host_name with status $status because of no rrd files\n" if ($sth ne "0E0");
# # # # 	    }
# # # # 	}
# # # #     }
# # # # }
# # # # 
# # # # sub cleanDeletedHosts {
# # # #     my $config = MindCommons::xmlfile_to_hash("config.xml");
# # # #     use Cwd 'abs_path';
# # # #     use File::Basename;
# # # #     use Mind_work::MuninWork;
# # # # 	    ## host name not defined: was deleted. set all files as not collected, so the stats thread will updated them with error. clean dirs
# # # # # 	    if (! defined $host_name) {
# # # # # 		DEBUG "Updating all files from hostid=$h_id with status EXIT_HOST_DELETE\n";
# # # # # 		my $sth = $db_h->do("update $config->{db_config}->{collected_file_table} set status=".EXIT_HOST_DELETE." WHERE host_id=$h_id") || die "Error $DBI::errstr\n";
# # # # # 		next;
# # # # # 	    }
# # # # 
# # # #     DEBUG "Clean all files from missing hosts\n";
# # # #     my $script_path = (fileparse(abs_path($0), qr/\.[^.]*/))[1]."";
# # # #     my $res = $db_h->selectall_arrayref("select c.name cust,h.name from customers c,hosts h where h.customer_id=c.id");
# # # #     my $customers;
# # # #     $customers->{@$_[0]}->{@$_[1]} = 1 foreach (@$res);
# # # #     $customers->{localdomain}->{localhost} = 1;
# # # # 
# # # # # rrd $Munin::Common::Defaults::MUNIN_DBDIR/$CUST/$HOST.$CUST-$PLUGIN
# # # #     foreach my $file (glob ("$Munin::Common::Defaults::MUNIN_DBDIR/*/*.rrd")){
# # # # 	my ($cust, $host, $cust2) = $file =~ /^$Munin::Common::Defaults::MUNIN_DBDIR\/(.*)\/(.*?)\.(.*?)-(.*).rrd$/;
# # # # 	if (! defined $host || ! defined $cust){
# # # # 	  DEBUG "Unknown file $file\n";
# # # # 	  next;
# # # # 	}
# # # # 	DEBUG "Delete rrd $file\n" if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
# # # # 	unlink $file if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
# # # #     }
# # # # # $Munin::Common::Defaults::MUNIN_SPOOLDIR/faker/$HOST.$CUST/datafile.$PLUGIN
# # # #     foreach my $file (glob ("$Munin::Common::Defaults::MUNIN_SPOOLDIR/faker/*/datafile.*")){
# # # # 	my ($cust, $host, $plugin) = $file =~ /^$Munin::Common::Defaults::MUNIN_SPOOLDIR\/faker\/(.*)\.(.*?)\/datafile\.(.*)$/;
# # # # 	if (! defined $host || ! defined $cust){
# # # # 	  DEBUG "Unknown file $file\n";
# # # # 	  next;
# # # # 	}
# # # # 	DEBUG "Delete datafile $file\n" if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
# # # # 	unlink $file if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
# # # #     }
# # # # # $Munin::Common::Defaults::MUNIN_CONFDIR/munin-conf.d/$HOST.$CUST
# # # #     foreach my $file (glob ("$Munin::Common::Defaults::MUNIN_CONFDIR/munin-conf.d/*")){
# # # # 	my ($host, $cust) = $file =~ /^$Munin::Common::Defaults::MUNIN_CONFDIR\/munin-conf.d\/(.*)\.(.*?)$/;
# # # # 	if (! defined $host || ! defined $cust){
# # # # 	  DEBUG "Unknown file $file\n";
# # # # 	  next;
# # # # 	}
# # # # 	DEBUG "Delete config $file\n" if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
# # # # 	unlink $file if ! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host};
# # # #     }
# # # # # $config->{dir_paths}->{uploads_dir}/$CUST/$HOST/
# # # # #     foreach my $file (glob ("$config->{dir_paths}->{uploads_dir}/*/*/*")){
# # # # # 	$config->{dir_paths}->{uploads_dir} =~ s/\/+/\//g;
# # # # # 	my ($cust, $host, $q) = $file =~ /^$config->{dir_paths}->{uploads_dir}\/(.*?)\/(.*?)\/(.*)$/;
# # # # # 	if (! defined $host || ! defined $cust){
# # # # # 	  DEBUG "Unknown file $file\n";
# # # # # 	  next;
# # # # # 	}
# # # # # 	if (! defined $customers->{$cust} || ! defined $customers->{$cust}->{$host}) {
# # # # # 	    DEBUG "Delete uploaded $file\n";
# # # # # 	    unlink $file;
# # # # # 	}
# # # # #     }
# # # # # $config->{dir_paths}->{filedone_dir}/$CUST/$HOST/
# # # # # $config->{dir_paths}->{fileerr_dir}/error/$CUST/$HOST/
# # # # # $script_path/$config->{dir_paths}->{plugins_conf_dir_postfix}/customers/$CUST/$HOST/$PLUGIN.conf
# # # # }
# # # # my $time_last_update = gmtime();
# # # # sub updateDatafile {
# # # #     use Mind_work::MuninWork;
# # # #     if (gmtime() - $time_last_update > 5){
# # # # 	DEBUG "Update global datafile\n";
# # # # 	cleanDeletedHosts;
# # # # 	$time_last_update = time;
# # # # # 	MuninWork::writedatafile;
# # # # 	my @all_lines;
# # # # 	foreach (MindCommons::find_files_recursively("$Munin::Common::Defaults::MUNIN_SPOOLDIR/faker/")){
# # # # 	    next if $_ !~ m/datafile/;
# # # # 	    open(FILE, "$_") || LOGDIE "can't open file $_: $!\n";
# # # # 	    my @new_lines = <FILE>;
# # # # 	    push @all_lines, @new_lines;
# # # # 	    close FILE;
# # # # 	}
# # # # 	open(FILE, ">$Munin::Common::Defaults::MUNIN_DBDIR/datafile") || LOGDIE "can't open file $_: $!\n";
# # # # 	print FILE join "", @all_lines;
# # # # 	close FILE;
# # # # 	return if -s "$Munin::Common::Defaults::MUNIN_DBDIR/datafile" < 50;
# # # #         DEBUG "Running global munin update\n";
# # # # 	system("/opt/munin/lib/munin-update", "--nofork") == 0 or WARN "Error running global munin-update\n";
# # # #         DEBUG "Running global munin limits\n";
# # # # 	system("/opt/munin/lib/munin-limits") == 0 or WARN "Error running global munin-limits\n";
# # # #         DEBUG "Running global munin html\n";
# # # # 	system("/opt/munin/lib/munin-html") == 0 or WARN "Error running global munin-html\n";
# # # #     }
# # # # }

sub get_customer_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select name from $config->{db_config}->{cust_table} where id=$id");
    return $arr_ref->[0];
}

sub get_customer_name_from_host_id {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("SELECT cust.name FROM $config->{db_config}->{cust_table} cust, $config->{db_config}->{host_table} hosts where cust.id=hosts.customer_id and hosts.id=$id");
    return $arr_ref->[0];
}

sub get_customer_id {
    my ($self, $name) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select id from $config->{db_config}->{cust_table} where name=".$db_h->quote($name));
    return $arr_ref->[0];
}

sub get_host_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select name from $config->{db_config}->{host_table} where id=$id");
    return $arr_ref->[0];
}

sub get_host_id {
    my ($self, $name, $cust_id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select id from $config->{db_config}->{host_table} where customer_id=$cust_id and name=".$db_h->quote($name));
    return $arr_ref->[0];
}

sub get_file_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select file_name from $config->{db_config}->{collected_file_table} where id=$id");
    return $arr_ref->[0];
}

sub get_md5_names {
    my ($self, $arr) = @_;
    my $existing = $db_h->selectall_hashref("select * from $config->{db_config}->{md5_names_table}", 'name');

    my $res;
    foreach (@$arr){
# 	next if $_ eq 'id' || $_ eq 'file_id' || $_ eq 'host_id' || $_ eq 'timestamp' || $_ eq 'group_by';
	LOGDIE "ce ma fac cu $_?\n" if ! defined $existing->{$_};
	$res->{$_} = $existing->{$_}->{md5};
    }
    return $res;
} 

sub getPluginInfo {
    my ($self, $file_id) = @_;
    my $hash_ref = $db_h->selectall_hashref("select b.* from $config->{db_config}->{collected_file_table} a, $config->{db_config}->{plugins_table} b where a.id=$file_id and b.id=a.plugin_id", 'id');
    LOGDIE "Too many rows returned:".Dumper($hash_ref) if scalar (keys %$hash_ref) != 1;
    return $hash_ref->{(keys %$hash_ref)[0]};
}

sub getColumns {
    my ($self, $table) = @_;
    my ($sth, @columns);
    $sth = $db_h->prepare("SELECT * FROM $table WHERE 1=0");
    $sth->execute;
    @columns = @{$sth->{NAME_lc}};
    $sth->finish;

    $sth = $db_h->prepare("SELECT * FROM $config->{db_config}->{md5_names_table} WHERE md5 in (". (join ",", map {$db_h->quote($_)} @columns).")");
    $sth->execute() || LOGDIE "Error $DBI::errstr\n";
    return $sth->fetchall_hashref('md5');
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
    my $md5_columns = ['file_id', 'timestamp', keys %$sha1_hash];
    my ($only_in_arr1, $only_in_arr2, $intersection) = MindCommons::array_diff( $md5_columns, $columns_e);

    while (my ($index, $sha1) = each @$only_in_arr1) {
	DEBUG "Adding new column $sha1 ($sha1_hash->{$sha1}) for table $table_name\n";
	$db_h->do("ALTER TABLE $table_name ADD $sha1 decimal(15,5)") || die "Error $DBI::errstr\n";
    }
    DEBUG "Releasing lock\n";
    $db_h->do("UNLOCK TABLES");
}

# sub timeFromLastUpdate {
#     my ($self, $status, $h_id) = @_;
#     DEBUG "Check if enough time has past from last file inserted\n";
#     return 0 if ! defined $last_time;
#     use Time::Local;
#     return (timegm(gmtime) - $last_time > 3) ? 1 : 0;
# }

# start work if no files exist for extract and all stats files done (no other stats pending). thread should be per inserted_in_tablename
sub getWorkForMunin {
    my ($self, $status) = @_;
    use Time::Local;
    use Munin::Common::Defaults;

#     my $hash = $db_h->selectall_hashref("select * from $config->{db_config}->{collected_file_table} where status=".SUCCESS_LAST, ['host_id', 'plugin_name']);
#   
# 	if (! glob("$Munin::Common::Defaults::MUNIN_DBDIR/$cust_name/$host_name.$cust_name-$plugin_name*")) {
# 	    TRACE "Plugin $plugin_name has no rrd files. Force reimport.\n";
# 	    $ret->{$tables} = $files_ids;
# 	    return $ret;
# 	}
    my $hash = $db_h->selectall_hashref("select * from $config->{db_config}->{collected_file_table} where status=$status", ['host_id', 'plugin_id', 'id']);
return;
# connect --url "https://localhost/api" --user "admin@internal" --password 'admin1234._'
# umask 0077; 
# MYTMP="$(mktemp -t ovirt-XXXXXXXXXX)"; 
# trap "chmod -R u+rwX \"${MYTMP}\" > /dev/null 2>&1; rm -fr \"${MYTMP}\" > /dev/null 2>&1" 0; 
# rm -fr "${MYTMP}" && mkdir "${MYTMP}" && tar -C "${MYTMP}" --no-same-permissions -o -x && "${MYTMP}"/setup -c 'ssl=true;management_port=54321' -O 'acasa' -t 2012-10-19T08:54:06  -S /tmp/ovirt-id_rsa_131ae4c2-7952-4c79-a89e-3aab346834ea -p 80 -b  -B ovirtmgmt  http://localhost:80/Components/vds/ http://localhost:80/Components/vds/ localhost 131ae4c2-7952-4c79-a89e-3aab346834ea False
    my $ret;
    foreach my $tables (keys %$hash){
	my $host = $hash->{$tables};
	LOGDIE "Database is corrupt (hosts): ".Dumper($tables, $host) if scalar keys %$host > 1;
	my $h_id = (keys %$host)[0];
	my $host_name = $self->get_host_name($h_id);
	my $cust_name = $self->get_customer_name_from_host_id($h_id);
	my $plugin = $hash->{$tables}->{$h_id};
	LOGDIE "Database is corrupt (plugins): ".Dumper($tables, $plugin) if scalar keys %$plugin > 1;
	my $plugin_name = (keys %$plugin)[0];

	if ( defined $db_h->selectrow_arrayref("select * from $config->{db_config}->{collected_file_table} where status<".$status." and status>".IGNORE." and host_id=$h_id") ) {
	    TRACE "We still have files to be processed by host $host_name (id=$h_id)\n";
	    next;
	}

	# wait 5 minutes from last file inserted
# 	my ($last_time) = @{ $db_h->selectrow_arrayref("select ".(timegm(gmtime))."-unix_timestamp(max(parse_done_time)) from $config->{db_config}->{collected_file_table} where status=$status and host_id=$h_id") };
# 	next if ! defined $last_time || $last_time < 300;

	my $files_ids = $hash->{$tables}->{$h_id}->{$plugin_name};
	foreach my $file_id (keys %$files_ids){
# 	    my $hash_ref = $db_h->selectall_hashref("select * from $config->{db_config}->{plugins_table} 
# 		    where host_id=".$db_h->quote($files_ids->{$file_id}->{host_id})." 
# 		      and stats_table=".$db_h->quote($files_ids->{$file_id}->{inserted_in_tablename}), ['id']  );

# 	    die "Only ONE row should have been returned: ".Dumper($hash_ref) if scalar keys %$hash_ref != 1;
# 	    my $plugin_id =(keys %$hash_ref)[0];
# 	    TRACE "Adding plugin id=$plugin_id for munin work\n";
# 	    $ret->{$plugin_id} = $hash_ref->{$plugin_id};
	}
    }
    return $ret;
}

sub getWorkForExtract {
    my ($self, $status) = @_;
    my $hash = $db_h->selectall_hashref("select * from $config->{db_config}->{collected_file_table} where status=$status and host_id>0 and customer_id>0", 'id');
    $self->removeDuplicatePaths($hash);
    TRACE "Got ".(scalar keys %$hash )." files for extract.\n" if scalar keys %$hash;
    return $hash;
}

sub getWorkForStatsParsers {
    my ($self, $status) = @_;
    my $hash = $db_h->selectall_hashref("select a.* from $config->{db_config}->{collected_file_table} a, $config->{db_config}->{plugins_table} b where a.status=$status and a.plugin_id=b.id and b.worker_type='statistics'", 'id');
    $self->removeDuplicatePaths($hash);
# print Dumper("qqq",keys %$hash,"www");
    foreach (keys %$hash) {
	$hash->{$_}->{plugin_info} = $self->getPluginInfo($_);
	$hash->{$_}->{customer_name} = $self->get_customer_name($hash->{$_}->{customer_id});
	$hash->{$_}->{host_name} = $self->get_host_name($hash->{$_}->{host_id});
    }

    TRACE "Got ".(scalar keys %$hash )." files for stats parser.\n" if scalar keys %$hash;
    return $hash;
}

sub getWorkForLogparser {
    my ($self, $status) = @_;
    my $hash = $db_h->selectall_hashref("select a.* from $config->{db_config}->{collected_file_table} a, $config->{db_config}->{plugins_table} b where status=$status and a.plugin_id=b.id and b.worker_type='logs'", 'id');
    $self->removeDuplicatePaths($hash);
    foreach (keys %$hash) {
	$hash->{$_}->{plugin_info} = $self->getPluginInfo($_);
	$hash->{$_}->{customer_name} = $self->get_customer_name($hash->{$_}->{customer_id});
	$hash->{$_}->{host_name} = $self->get_host_name($hash->{$_}->{host_id});
    }

    TRACE "Got ".(scalar keys %$hash )." files for log parser.\n" if scalar keys %$hash;
    return $hash;
}

sub removeDuplicatePaths {
    my ($self, $hash) = @_;
    my $paths;
    foreach my $key (keys %$hash) {
	if (defined $paths->{$hash->{$key}->{file_name}} && -f $hash->{$key}->{file_name}) {
	    TRACE "delete $key if we still have the file\n";
	    delete $hash->{$key};
	} else {
	    $paths->{$hash->{$key}->{file_name}} = 1;
	}
    }
}

sub disconnect {
    $db_h->disconnect(); 
}

return 1;
