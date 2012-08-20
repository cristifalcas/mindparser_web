package MuninWork;
## needs from rc.local
## disable graph generation in  /opt/munin/bin/munin-cron
# in munin-async I print "\n.\n" instead of ".\n" after "print $spoolreader->fetch($last_epoch);"
# Also, in this case, I think that $correct variable should be initialized with 0 in Munin/Master/Node.pm at line 254

use warnings;
use strict;

use Munin::Node::SpoolWriter;
use Munin::Common::Defaults;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use File::Path qw(make_path);

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({ level   => $INFO,
#                            file    => ">>test.log" 
			   layout   => "%d [%5p] (%6P) %m%n",
});

use constant {
    EXIT_STATUS_NA	=>-1,
    EXIT_IGNORE		=> 0,
    EXIT_STATS_SUCCESS	=> 1,
    EXIT_MUNIN_SUCCESS	=> 2,
    EXIT_PARSE_SUCCESS	=> 3,
    EXIT_STATS_EXPECTS	=> 0,
    EXIT_MUNIN_EXPECTS	=> 1,
    EXIT_PARSE_EXPECTS	=> 0,
    EXIT_NO_FILE	=> 100,
    EXIT_WRONG_TYPE	=> 102,
    EXIT_NO_LINES	=> 110,
    EXIT_WRONG_MINE	=> 122,
    EXIT_EXTR_ERR	=> 150,
    EXIT_NO_ROWS	=> 200,
};

use Mind_work::SqlWork;
use Mind_work::MindCommons;
my $config = MindCommons::xmlfile_to_hash("config.xml");
my $db_h;
my ($spoolwriter, $munin_dbdir, $intervalsize, $retaincount, $rate);
my ($conf_file, $hostname, $work_dir, $customer, $host, $plugins_conf_dir);

# "$Munin::Common::Defaults::MUNIN_CONFDIR"
# "$Munin::Common::Defaults::MUNIN_SPOOLDIR"
# "$Munin::Common::Defaults::MUNIN_DBDIR"

sub writeConfFiles {
    ##fake dirs in file for real munin
    my $fake_dir = "$Munin::Common::Defaults::MUNIN_SPOOLDIR/faker/$hostname/";
    open(MYOUTFILE, ">$Munin::Common::Defaults::MUNIN_CONFDIR/munin-conf.d/$hostname") || LOGDIE "can't open file $Munin::Common::Defaults::MUNIN_CONFDIR/munin-conf.d/$hostname: $!\n";
    print MYOUTFILE "[$hostname]
      update yes
      address ssh://munin\@localhost /opt/munin/lib/munin-async --spoolfetch --spooldir $fake_dir";
    close(MYOUTFILE);
    if (! -d $fake_dir) {make_path $fake_dir or LOGDIE "can't make dir $fake_dir: $!\n";}

    ## real conf file for us
    open(MYOUTFILE, ">$conf_file") || LOGDIE "can't open file $conf_file: $!\n";
    print MYOUTFILE "rundir $work_dir
dbdir $work_dir

[$hostname]
      update yes
      #worker_timeout 5
      address ssh://munin\@localhost /opt/munin/lib/munin-async --spoolfetch --spooldir $work_dir --cleanup";
    close(MYOUTFILE);
}

sub setGlobalUpdateRate {
    my ($spool_dir, $plugin_name) = @_;
    use Fcntl;
    use DB_File;
    my %hash;
    tie (%hash, 'DB_File', "$spool_dir/plugin_rates", O_RDWR|O_CREAT, 0666) or LOGDIE "$!";
    $hash{$plugin_name} = $rate;
    untie(%hash);
}

sub initVars {
    my $host_id = shift;
    use Cwd 'abs_path';
    use File::Basename;

    $munin_dbdir = "$Munin::Common::Defaults::MUNIN_DBDIR";
    $intervalsize = 86400;
    $retaincount = 1;
    $rate = 300;

    $customer = getCustomerName($host_id);
    $host = getHostName($host_id);
    $hostname = "$host.$customer";

    my $filetmp_dir = $config->{dir_paths}->{filetmp_dir};
    $work_dir = "$filetmp_dir/munin/$customer\_$host/";
    my $script_path = (fileparse(abs_path($0), qr/\.[^.]*/))[1]."";

    $plugins_conf_dir = "$script_path/".$config->{dir_paths}->{plugins_conf_dir_postfix};

    make_path $work_dir || LOGDIE "can't create spool dir $work_dir.\n";
    $spoolwriter = Munin::Node::SpoolWriter->new(
	spooldir => $work_dir,
	interval_size => $intervalsize,
	interval_keep => $retaincount,
	hostname  => $hostname,
    );
    
    $conf_file = "$work_dir/$customer\_$host.conf";
}

sub getCustomerName {
    my $host_id = shift;
    return $db_h->selectrow_array("select c.name from hosts h, customers c where h.customer_id=c.id and h.id=$host_id");
}

sub getHostName {
    my $host_id = shift;
    return $db_h->selectrow_array("select name from hosts where id=$host_id");
}

sub getColumns {
    my $table = shift;
    my ($sth, @columns);
    $sth = $db_h->prepare("SELECT * FROM $table WHERE 1=0");
    $sth->execute;
    @columns = @{$sth->{NAME}}; # or NAME_lc if needed
    $sth->finish;

    $sth = $db_h->prepare("SELECT * FROM $config->{db_config}->{md5_names_table} WHERE md5 in (". (join ",", map {$db_h->quote($_)} @columns).")");
    $sth->execute() || LOGDIE "Error $DBI::errstr\n";
    return $sth->fetchall_hashref('md5');
}

sub copyOldFiles {
    my ($direction, $plugin_name) = @_;
    use Storable;
    my $storable = sprintf ('%s/state-%s.storable', $munin_dbdir, "$customer-$hostname");#"state-$customer-$host.$customer.storable";
    my $last_timestamp = 0;
    my ($name,$dir,$suffix) = fileparse($storable, qr/\.[^.]*/);

    use File::Path 'rmtree';
    use File::Copy;
    use File::Copy::Recursive qw(dircopy rcopy);
    if ($direction == 1) {
	rmtree ($work_dir);
	make_path $work_dir || LOGDIE "can't create spool dir $work_dir.\n";
	if (-f $storable || scalar (glob("$munin_dbdir/$customer/$host.$customer-*"))){
	    copy ($storable, "$work_dir/$name$suffix") or LOGDIE "can't get storable file: $!\n";
	    foreach (glob("$munin_dbdir/$customer/$host.$customer-$plugin_name-*")){
		my $timestamp = `rrdtool info $_` || LOGDIE "can't run rrdtool\n";
		($timestamp) = grep {m/^last_update/} (split /\n/, $timestamp);
		$timestamp =~ s/^last_update\s*=\s*//;
		$last_timestamp = $timestamp+0 if $timestamp > $last_timestamp || $last_timestamp == 0;
		DEBUG "Using timestamp $last_timestamp\n";
		make_path "$work_dir/$customer" || LOGDIE "can't create dir $work_dir/$customer.\n";
		copy ($_, "$work_dir/$customer/") or LOGDIE "can't copy rrd file $_: $!\n";
	    };
	    my $storable_hash = eval { Storable::retrieve("$work_dir/$name$suffix"); };
	    $storable_hash->{spoolfetch} = $last_timestamp;
	    Storable::nstore($storable_hash, "$work_dir/$name$suffix");
	}
    } elsif ($direction == 2) {
	dircopy("$work_dir/$customer", "$munin_dbdir/$customer") or LOGDIE "can't copy dir $work_dir/$customer: $!\n";
	copy ("$work_dir/$name$suffix", $storable) or LOGDIE "can't copy storable file: $!\n";
	if (-f "$work_dir/datafile") {
	    open(FILE, "$work_dir/datafile") or LOGDIE "Can't open file for reading: $!\n";
	    my @new_lines = <FILE>;
	    close(FILE);

	    open(FILE, ">>$munin_dbdir/datafile") or LOGDIE "Can't open file for writing: $!\n";
	    print FILE "\n";
	    foreach (@new_lines) {
		print FILE $_;
	    }
	    close(FILE);
	}
    } else {
	LOGDIE "unknown direction = $direction\n";
    }
    return $last_timestamp ? $last_timestamp-$rate : $last_timestamp;
}

sub run {
    my ($prev_dbh, $host_id, $tables) = @_;
    $db_h = $prev_dbh->getDBI_handler();
    initVars($host_id);
    foreach my $stats_table (keys %$tables) {
	INFO "Start updating $stats_table from host id=$host_id, name=$hostname\n";
	my $colums_name = getColumns($stats_table);
	my ($type) = $stats_table =~ m/^([^_]+)/;
	my $plugin_name = "test_$type\_$host\_$customer\_mind";  # MUST contain only 0-9a-z_
	if (! -f "$plugins_conf_dir/$plugin_name.conf" || ! -s "$plugins_conf_dir/$plugin_name.conf") {
	    open(FILE, ">$plugins_conf_dir/$plugin_name.conf") or LOGDIE "Can't open file for writing: $!\n";
	    foreach (keys %$colums_name) {
		next if $colums_name->{$_}->{name} =~ m/^\s*Date\s*$/i || $colums_name->{$_}->{name} =~ m/^\s*Time\s*$/i;
		print FILE $colums_name->{$_}->{name}."\n";
	    }
	    close(FILE);
	}

	my $graph_name;
	open(FILE, "$plugins_conf_dir/$plugin_name.conf") or LOGDIE "Can't open file for reading: $!\n";
	my $section = "Other";
	foreach my $name (<FILE>) {
	    $name =~ s/(\n*|\r*)//g;
	    if ($name =~ m/^\s*\[(.*?)\]\s*$/){
		$section = $1;
	    } else {
		$graph_name->{$name} = $section;
	    }
	}
	close(FILE);

	my $from_time = copyOldFiles(1, $plugin_name);

	$db_h->{'mysql_use_result'} = 1;
	$db_h->{'mysql_auto_reconnect'} = 1;
	DEBUG "Getting lines from $stats_table with host_id=$host_id, timestamp=$from_time\n";
	my $sth = $db_h->prepare("SELECT * FROM $stats_table WHERE host_id=$host_id and timestamp>=$from_time order by timestamp");
	$sth->execute() || LOGDIE "Error $DBI::errstr\n";

	my $first = 1;
	while (my $aref = $sth->fetchall_arrayref({}, 1000) ){
	    return EXIT_NO_ROWS if ! (scalar @$aref);
	    if ($first){
		writeConfFiles();
		$first = 0;
	    }
	    unlink glob ("$work_dir/munin-daemon.$plugin_name.*");
	    DEBUG "got nr rows : ".(scalar @$aref)."\n";
	    foreach my $row (@$aref){
		my @output_rows = (
    # 		"graph_title $stats_table",
    # 		"graph_vlabel eceva pt vlabel",
		    "graph_scale no",
		    "graph_category $customer"."_$type",
		    "graph_info $stats_table",
		    "update_rate $rate",
		    "graph_data_size custom 115200",
    # 		"hostname $hostname",
		    );
	    my $q;
    # http://membersresource.dialogic.com/_releases/ss7/ProductSpecific/SS7G3x/Software/ss7g30-siu.tgz
    # cntms:module=mtp,imask=0xffffffff,omask=0xffffffff,mmask=0xffffffff,active=y;
    # cntms:module=isup,imask=0xffffffff,omask=0xffffffff,mmask=0xffffffff,active=y;
	    foreach my $md5 (keys %$row) {
		next if ! defined $row->{$md5} || $md5 =~ m/^(id|host_id|file_id|timestamp|date|time)$/i;
		my $val = defined $row->{$md5} ? $row->{$md5} : "undef";
# 		my $name = $md5 =~ m/^(id|host_id|file_id|timestamp|date|time)$/i ? $md5 : $colums_name->{$md5}->{name};
		my $name = $colums_name->{$md5}->{name};
		push @{$q->{$graph_name->{$name}}}, (@output_rows, "graph_title $graph_name->{$name}") if ! defined $q->{$graph_name->{$name}};
		push @{$q->{$graph_name->{$name}}}, (
# 		    "multigraph $graph_name->{$name}",
		    "$md5.label $name",
		    "$md5.info $name",
		    "$md5.value $val",
		);
	    }

    foreach (keys %$q) {
    my $plugin_name_ok = $_;
    $plugin_name_ok =~ s/[^a-z0-9_]/_/gi;
    $spoolwriter->write($row->{timestamp}, $plugin_name_ok, $q->{$_}) ;
    }
	    }
	    INFO "Running munin-update for $hostname\n";
	    system("/opt/munin/lib/munin-update", "--config_file=$conf_file", "--host", "$hostname"); #, "--debug"
	}
	copyOldFiles(2);
    # exit 1;
    }
    return EXIT_MUNIN_SUCCESS;
}

# http://munin-monitoring.org/wiki/MultigraphSampleOutput
# http://munin-monitoring.org/wiki/protocol-config
# http://munin-monitoring.org/wiki/format-graph_data_size
# my $output_rows = [
# ## asking for config
# ## create custom RRAs ???
# # "custom 576, 6 432, 24 540, 288 450" 
# ## If set, the arguments will be passed on to rrdcreate.  not implemented yet it seems
# #   "create_args step=2*$plugin_rate",
# ## graph_args the arguments will be passed on to rrdgraph. 
# # "graph_args --lower-limit 0 --base 1000 -l 0\n",
# 
#   "graph_title table_name statistics",
# #   "graph_vlabel entropy (bytes)_doco",
# # graph_total If set, summarizes all the data sources' values and use the value of graph_total? as label. 
# # graph_scale	Default on/yes. If set, disables automatic unit scaling of values. 
#   "graph_category customer_RTS",
#   "graph_scale no",
#   "graph_info table_name",
# 
# #   "md5name.type GAUGE|COUNTER|DERIVE|ABSOLUTE",
# # .warning min:max, min: or :max #if low or high are reached
# # .critical min:max, min: or :max 
# # {fieldname}.draw	How to draw the values from the data source: AREA, LINE1, LINE2, LINE3 or STACK. Defaults to LINE2. From munin 2.0, the default is LINE1. From 1.3.3 munin additionally supports LINESTACK1, LINESTACK2, LINESTACK3 (or floating point thickness) as well as AREASTACK 
# # {fieldname}.max?	Maximum value. If the fetched value is above "max", it will be discarded.
# # {fieldname}.min?	Minimum value. If the fetched value is below "min", it will be discarded. 
# # {fieldname}.line	value[:color[:label]] Adds a horizontal line with the fieldname's colour (HRULE) at the value defined. Will not show if outside the graph's scale.
# # {fieldname}.oldname?	Specifies the previous name of this fieldname. If this attribute is available the first time the master sees the renamed field it will try to preserve the history from oldname. Available from 1.4.
#   "md5name.label name of md5name",
#   "md5name.info name of md5name",
#   "md5name.update_rate plugin_rate", #this is written in the db_file for the entire plugin; is this only for grapg?
#   
# ## asking for data
#   "md5name.value 1.192547287348511",
# ];

# graph_data_size custom 1y, 1h for 2y, 1d for 5y
# The graph_data_size can be either:
# - global, defined in munin.conf
# - per host, defined in munin.conf in a host section
# - per plugin, defined directly by the plugin in the config "section".

# # Example:
# config df
# graph_title Disk usage in percent
# graph_args --upper-limit 100 -l 0
# graph_vlabel %
# graph_scale no
# graph_category disk
# devtmpfs.label /dev
# devtmpfs.warning 92
# devtmpfs.critical 98
# .
# fetch df
# devtmpfs.value 0
# .

return 1;
