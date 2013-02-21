package SqlWork;

use warnings;
use strict;
$| = 1; 
$SIG{__WARN__} = sub { die @_ };

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
    $db_h = DBI->connect("DBI:mysql:$config->{db_config}->{db_database}:wikitiki.mindsoft.com:3306", $config->{db_config}->{db_user}, $config->{db_config}->{db_pass},
	{ ShowErrorStatement => 1,
          AutoCommit => 1,
          RaiseError => 1,
          mysql_use_result => 0,
          mysql_enable_utf8 => 1,
	  mysql_auto_reconnect => 1,
          PrintError => 1, }) || LOGDIE "Could not connect to database: $DBI::errstr";
    bless($self, $class);
    return $self;
}

sub getDBI_handler {
    return $db_h;
}

sub clean_existing_files {
    my ($self, $plugin_id) = @_;
    $db_h->do("update $config->{db_config}->{collected_file_table} 
		      set status=".EXIT_STATUS_NA)
	  || LOGDIE "Error $DBI::errstr\n";
}

sub insertFile {
    my ($self, $file_hash, $status) = @_;
    DEBUG "Add to db file $file_hash->{file_info}->{name} with status $status.\n";
    my $cust_id = $self->get_customer_id($file_hash->{machine}->{customer}) || EXIT_STATUS_NA;
    my $host_id = $self->get_host_id($file_hash->{machine}->{host}, $cust_id) || EXIT_STATUS_NA;

    my $cols = ['customer_id', 'host_id', 'plugin_id', 'file_name', 'file_md5', 'size', 'status'];
    my $vals = [$cust_id, $host_id, EXIT_STATUS_NA, $self->getQuotedString($file_hash->{file_info}->{name}), $self->getQuotedString($file_hash->{file_info}->{md5}), $file_hash->{file_info}->{size}, $status ];

    $self->insertRowsTable($config->{db_config}->{collected_file_table}, $cols, ($vals))
}

sub increasePluginQueue {
    my ($self, $plugin_id) = @_;
    $db_h->do("update $config->{db_config}->{plugins_table} 
		      set files_queue=IFNULL(files_queue, 0) + 1 WHERE id=$plugin_id")
	  || LOGDIE "Error $DBI::errstr\n";
}

sub decreasePluginQueue {
    my ($self, $plugin_id) = @_;
    $db_h->do("update $config->{db_config}->{plugins_table} 
		      set files_queue=files_queue - 1 WHERE id=$plugin_id")
	  || LOGDIE "Error $DBI::errstr\n";
}

sub nulifyPluginsQueue {
    my ($self) = @_;
    $db_h->do("update $config->{db_config}->{plugins_table} set files_queue=0") || LOGDIE "Error $DBI::errstr\n";
}

sub getPluginQueue {
    my ($self, $plugin_id) = @_;
    my $res = $db_h->selectrow_arrayref("select id from $config->{db_config}->{plugins_table} WHERE id=$plugin_id") || LOGDIE "Error $DBI::errstr\n";
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
    $db_h->do("update $config->{db_config}->{collected_file_table} set ".(join ", ", @txt)." WHERE id=$fileid") || LOGDIE "Error $DBI::errstr\n";
}

sub insertRowsTable {
    my ($self, $table_name, $cols, @vals) = @_;
    return if ! @vals || ! scalar @vals;
    ## @vals is an array of array refs
    LOGDIE "We can't insert those: ".Dumper($table_name, $cols, @vals) if scalar @$cols != scalar @{ $vals[0] };
    $_ = join ",", @$_ foreach @vals;
    $db_h->do("INSERT IGNORE INTO $table_name (".(join ",", @$cols).") VALUES (".(join ") , (", @vals).")") || LOGDIE "Error $DBI::errstr\n";
}

sub createStatsTable {
    my ($self, $table_name) = @_;
    DEBUG "create new table $table_name\n";
    $db_h->do("CREATE TABLE IF NOT EXISTS $table_name (
	file_id int not null,
	host_id int not null,
	timestamp int not null,
	group_by varchar(20),
	UNIQUE (host_id, timestamp, group_by),
	FOREIGN KEY (host_id) REFERENCES $config->{db_config}->{host_table}(id))") || LOGDIE "Error $DBI::errstr\n";
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
    my ($self, $table_name, $cols_old, $vals_old) = @_;
    use Storable qw(dclone);
    my $cols = dclone($cols_old);
    my $vals = dclone($vals_old);

    LOGDIE "Not good (cols nr <> vals nr)\n" if scalar @$cols != scalar @$vals;
    my @sel;
    push @sel, "$_=".(shift @$vals) foreach @$cols;
    my $res = $db_h->selectrow_arrayref("select id from $table_name where ".(join " and ", @sel));
    LOGDIE "Error $DBI::errstr\n" if defined $DBI::errstr;
#     LOGDIE "Strange shit:\n".Dumper($table_name, $cols_old, $vals_old) if ! defined $res->[0];
    return  $res->[0];
}

sub getQuotedString {
    my ($self, $string) = @_;
    return $db_h->quote($string);
}

sub setPluginDefaults {
    my ($self, $plugin_id) = @_;
    my $plugin_info = $db_h->selectrow_arrayref("select inserted_in_tablename, plugin_name from $config->{db_config}->{plugins_table} where id=$plugin_id");
    my ($sth, @columns);
    $sth = $db_h->prepare("SELECT * FROM $plugin_info->[0] WHERE 1=0");
    $sth->execute;
    @columns = @{$sth->{NAME_lc}};
    $sth->finish;

    my $defaults = $db_h->selectrow_arrayref("select * from $config->{db_config}->{plugins_conf_default} where plugin_name='$plugin_info->[1]'");
LOGDIE "We don't do anything bitch" if defined $defaults;
    my ($only_in_a, $only_in_b, $common) = MindCommons::array_diff(\@columns, $defaults);

    my ($only_here, $ignore1, $ignore2) = MindCommons::array_diff(\@columns, $columns_header);
    $db_h->do("INSERT IGNORE INTO $config->{db_config}->{plugins_conf} (plugin_id, section_name, md5_name, extra_info) VALUES ($plugin_id, 'Not configured', '$_', '')") foreach @$only_here;
}

sub getPluginConf {
    my ($self, $plugin_id) = @_;
    my $hash = $db_h->selectall_hashref("select * from $config->{db_config}->{plugins_conf} where plugin_id=$plugin_id order by section_name", ['md5_name']);
    my $res;
    foreach my $key (keys %$hash){
	my $section = $hash->{$key}->{section_name};
	DEBUG "Using section $section.\n";
	my @extra_info = split ":", $hash->{$key}->{extra_info};
	$res->{$section}->{$key} = \@extra_info;
    }
    return $res;
}

sub getPluginInfo {
    my ($self, $file_id) = @_;
    my $hash_ref = $db_h->selectall_hashref("select b.* from $config->{db_config}->{collected_file_table} a, $config->{db_config}->{plugins_table} b where a.id=$file_id and b.id=a.plugin_id", 'id');
    LOGDIE "Too many rows returned:".Dumper($hash_ref) if scalar (keys %$hash_ref) != 1;
    return $hash_ref->{(keys %$hash_ref)[0]};
}

sub getColumnList {
  my($self, $table) = @_;

  my $sth = $db_h->prepare("SELECT * FROM $table WHERE 1=0");
  $sth->execute || LOGDIE "Error $DBI::errstr\n";
  my $cols = $sth->{NAME}; # or NAME_lc if needed
  $sth->finish;
  return $cols;
}

sub getGroupsForPlugin {
    my ($self, $plugin_id) = @_;
    my $res = $db_h->selectrow_arrayref("select inserted_in_tablename, host_id from $config->{db_config}->{plugins_table} WHERE id=$plugin_id") || LOGDIE "Error $DBI::errstr\n";
    return $db_h->selectall_arrayref("SELECT DISTINCT group_by FROM $res->[0] WHERE host_id=$res->[1]") || LOGDIE "Error $DBI::errstr\n";
}

sub getCustomers {
    my $self = shift;
    my $hash;
    my $query = $db_h->prepare("select * from $config->{db_config}->{cust_table}");
    $query->execute() || LOGDIE "Error $DBI::errstr\n";
    while (my @row = $query->fetchrow_array ){
	LOGDIE "lame 1\n".Dumper(@row) if @row != 2;
	my $query_h = $db_h->prepare("select * from $config->{db_config}->{host_table} where customer_id=$row[0]");
	$query_h->execute() || LOGDIE "Error $DBI::errstr\n";
	while (my @row_h = $query_h->fetchrow_array ){
	    LOGDIE "lame 2\n".Dumper(@row_h) if @row_h != 4;
	    $hash->{$row[1]}->{'id'} = $row[0];
	    $hash->{$row[1]}->{'hosts'}->{$row_h[2]} = $row_h[0];
	}
    }
    return $hash;
}

# # # # 
# # # # sub cleanDeletedHosts {
# # # #     my $config = MindCommons::xmlfile_to_hash("config.xml");
# # # #     use Cwd 'abs_path';
# # # #     use File::Basename;
# # # #     use Mind_work::MuninWork;
# # # # 	    ## host name not defined: was deleted. set all files as not collected, so the stats thread will updated them with error. clean dirs
# # # # # 	    if (! defined $host_name) {
# # # # # 		DEBUG "Updating all files from hostid=$h_id with status EXIT_HOST_DELETE\n";
# # # # # 		my $sth = $db_h->do("update $config->{db_config}->{collected_file_table} set status=".EXIT_HOST_DELETE." WHERE host_id=$h_id") || LOGDIE "Error $DBI::errstr\n";
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

sub get_names_from_md5 {
    my ($self, $arr) = @_;
    my $names = $db_h->selectall_hashref("select * from $config->{db_config}->{md5_col_names} where md5 in ( '".(join "', '", @$arr)."' )", 'md5');
    my $res;
    $res->{$_} = $names->{$_}->{name} foreach (keys %$names);
    return $res;
}

sub get_md5_names {
    my ($self, $arr) = @_;
    my $existing = $db_h->selectall_hashref("select * from $config->{db_config}->{md5_col_names}", 'name');

    my $columns_header_string = join "\t", @$columns_header;
    my $res;
    foreach (@$arr){
	next if $columns_header_string =~ m/$_/;
# 	next if $_ eq 'id' || $_ eq 'file_id' || $_ eq 'host_id' || $_ eq 'timestamp' || $_ eq 'group_by';
	LOGDIE "ce ma fac cu $_?\n" if ! defined $existing->{$_};
	$res->{$_} = $existing->{$_}->{md5};
    }
    return $res;
} 

sub get_plugin_update_rate {
    my ($self, $plugin_id) = @_;
    return 300;
}

sub add_new_columns {
    my ($self, $table_name, $arr) = @_;
    DEBUG "Acquiring lock\n";
    $db_h->do("LOCK TABLES $table_name WRITE, $config->{db_config}->{md5_col_names} WRITE");
    my $sha1_hash;
    $sha1_hash->{"x_".MindCommons::get_string_sha($_)} = $_ foreach (@$arr);
    my $md5_existing = $db_h->selectall_hashref("select * from $config->{db_config}->{md5_col_names}", 'md5');
    foreach (keys %$sha1_hash){
	next if defined $md5_existing->{$_};
	INFO "Adding new rows $sha1_hash->{$_} ($_) in $config->{db_config}->{md5_col_names}\n";
	$db_h->do("INSERT IGNORE INTO $config->{db_config}->{md5_col_names} (md5, name) VALUES ('$_', '$sha1_hash->{$_}')");
    }
    my $columns_e = getColumnList($self, $table_name);
    my $md5_columns = ['file_id', 'timestamp', keys %$sha1_hash];
    my ($only_in_arr1, $only_in_arr2, $intersection) = MindCommons::array_diff( $md5_columns, $columns_e);

    while (my ($index, $sha1) = each @$only_in_arr1) {
	DEBUG "Adding new column $sha1 ($sha1_hash->{$sha1}) for table $table_name\n";
	$db_h->do("ALTER TABLE $table_name ADD $sha1 decimal(15,5)") || LOGDIE "Error $DBI::errstr\n";
    }
    DEBUG "Releasing lock\n";
    $db_h->do("UNLOCK TABLES");
}

sub setNeedsUpdate {
    my ($self, $plugin_id, $status) = @_;
    $db_h->do("update $config->{db_config}->{plugins_table} set needs_update=$status WHERE id=$plugin_id") || LOGDIE "Error $DBI::errstr\n";
}

sub getMuninNoRRD {
    my $self = shift;
    use Munin::Master::Utils;
    my $munin_config = munin_readconfig_base("$Munin::Common::Defaults::MUNIN_CONFDIR/munin.conf");

    my $custs = getCustomers();
    foreach my $cust_name (sort keys %$custs) {
	next if $custs->{$cust_name}->{id} < 1;
	my $hosts = $custs->{$cust_name}->{hosts};
	foreach my $host_name (sort keys %$hosts) {
	    my $hash = $db_h->selectall_hashref("select * from $config->{db_config}->{plugins_table} where host_id=$hosts->{$host_name} and customer_id=$custs->{$cust_name}->{id}", ['id']);
	    foreach my $plugin_id ( sort keys %$hash) {
		my @files = glob("$munin_config->{dbdir}/$cust_name/$host_name.$cust_name-$hash->{$plugin_id}->{plugin_name}*");
		if (! scalar @files) {
		    DEBUG "Plugin $hash->{$plugin_id}->{plugin_name} has no rrd files. Force reimport.\n";
		    $self->setNeedsUpdate($plugin_id, 1);
		}
	    }
	}
    }
}

# start work if no files exist for extract and all stats files done (no other stats pending). thread should be per inserted_in_tablename
sub getWorkForMunin {
    my ($self, $status) = @_;
    $self->getMuninNoRRD();
    use Time::Local;
    my $res = $db_h->selectall_hashref("select * from $config->{db_config}->{plugins_table} where files_queue=0 and needs_update>0", ['id']);
#     use Storable qw(dclone);
#     my $return = dclone($res);
    foreach (sort keys %$res){
# 	wait 5 minutes from last file inserted
	my ($last_time) = @{ $db_h->selectrow_arrayref("select ".(timegm(gmtime))."-unix_timestamp(max(parse_done_time)) from $config->{db_config}->{collected_file_table} where plugin_id=$_") };
	delete $res->{$_} if (defined $last_time && $last_time < 30);
    }
    return $res;
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
    foreach (keys %$hash) {
	$hash->{$_}->{plugin_info} = $self->getPluginInfo($_);
	$hash->{$_}->{customer_name} = $self->get_customer_name($hash->{$_}->{customer_id});
	$hash->{$_}->{host_name} = $self->get_host_name($hash->{$_}->{host_id});
    }
# INFO Dumper($hash);
    TRACE "Got ".(scalar keys %$hash )." files for stats parser.\n" if scalar keys %$hash;
    return $hash;
}

sub getWorkForLogParsers {
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
