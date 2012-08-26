package MuninWork;
## needs from rc.local
## disable graph generation in  /opt/munin/bin/munin-cron
# in /opt/munin/lib/munin-async I print "\n.\n" instead of ".\n" after "print $spoolreader->fetch($last_epoch);"
# in /usr/local/share/perl5/Munin/Master/Node.pm at line 254 $correct variable should be initialized with 0
# /usr/local/share/perl5/Munin/Master/ProcessManager.pm : accept_timeout  => 2,

use warnings;
use strict;

use Munin::Node::SpoolWriter;
use Munin::Common::Defaults;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use File::Path qw(make_path);
use File::Path 'rmtree';
use File::Copy;

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
    EXIT_HOST_DELETE	=> 140,
    EXIT_WRONG_TYPE	=> 102,
    EXIT_WRONG_NAME	=> 102,
    EXIT_NO_LINES	=> 110,
    EXIT_WRONG_MINE	=> 122,
    EXIT_EXTR_ERR	=> 150,
    EXIT_NO_ROWS	=> 200,
    EXIT_NO_RRD		=> 210,
    EXIT_MAIN_ERROR	=> 1000,
    EXIT_MUNIN_ERROR	=> 130,
};

use Mind_work::SqlWork;
use Mind_work::MindCommons;
my $config = MindCommons::xmlfile_to_hash("config.xml");
my $db_h;
my ($spoolwriter, $munin_dbdir, $munin_db_fake_dir, $rate);
my ($conf_file, $hostname, $work_dir, $customer, $host, $plugins_conf_dir);

# "$Munin::Common::Defaults::MUNIN_CONFDIR"
# "$Munin::Common::Defaults::MUNIN_SPOOLDIR"
# "$Munin::Common::Defaults::MUNIN_DBDIR"

sub writeConfFiles {
    ##fake dirs in file for real munin
    open(MYOUTFILE, ">$Munin::Common::Defaults::MUNIN_CONFDIR/munin-conf.d/$hostname") || LOGDIE "can't open file $Munin::Common::Defaults::MUNIN_CONFDIR/munin-conf.d/$hostname: $!\n";
    print MYOUTFILE "[$hostname]
      update yes
      address ssh://munin\@localhost /opt/munin/lib/munin-async --spoolfetch --spooldir $munin_db_fake_dir\n";
    close(MYOUTFILE);
    if (! -d $munin_db_fake_dir) {make_path $munin_db_fake_dir or LOGDIE "can't make dir $munin_db_fake_dir: $!\n";}

    ## real conf file for us
    open(MYOUTFILE, ">$conf_file") || LOGDIE "can't open file $conf_file: $!\n";
    print MYOUTFILE "rundir $work_dir
dbdir $work_dir

[$hostname]
      update yes
      address ssh://munin\@localhost /opt/munin/lib/munin-async --spoolfetch --spooldir $work_dir --cleanup\n";
    close(MYOUTFILE);
}

# sub setGlobalUpdateRate {
#     my ($spool_dir, $plugin_name) = @_;
#     use Fcntl;
#     use DB_File;
#     my %hash;
#     tie (%hash, 'DB_File', "$spool_dir/plugin_rates", O_RDWR|O_CREAT, 0666) or LOGDIE "$!";
#     $hash{$plugin_name} = $rate;
#     untie(%hash);
# }

sub initVars {
    my ($host_id, $plugin_name) = @_;
    use Cwd 'abs_path';
    use File::Basename;

    $munin_dbdir = "$Munin::Common::Defaults::MUNIN_DBDIR";
    ## munin will not update previous day on day change at all otherwise
    my $intervalsize = 86400 * 365;
    my $retaincount = 365;
    $rate = 300;

    $customer = getCustomerName($host_id);
    $host = getHostName($host_id);
    $hostname = "$host.$customer";
    $munin_db_fake_dir = "$Munin::Common::Defaults::MUNIN_SPOOLDIR/faker/$hostname/";

    my $filetmp_dir = $config->{dir_paths}->{filetmp_dir};
    $work_dir = "$filetmp_dir/munin/$customer\_$host\_$plugin_name/";
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
    my $plugin_name = shift;
#     use Storable;
#     my $storable = sprintf ('%s/state-%s.storable', $munin_dbdir, "$customer-$hostname");#"state-$customer-$host.$customer.storable";
    my $last_timestamp = 0;
#     my ($name,$dir,$suffix) = fileparse($storable, qr/\.[^.]*/);

#     use File::Copy::Recursive qw(dircopy rcopy);
#     if ($direction == 1) {
	rmtree ($work_dir);
	make_path $work_dir || LOGDIE "can't create spool dir $work_dir.\n";
# 	    symlink("$munin_dbdir/$customer/", "$work_dir/") or LOGDIE "can't symlink rrd dir: $!\n";
	make_path "$munin_dbdir/$customer/" if ! -d "$munin_dbdir/$customer/";
	system("ln", "-s", "$munin_dbdir/$customer/", "$work_dir/") == 0 || LOGDIE "can't symlink rrd dir: $!\n";
# 	if (scalar (glob("$munin_dbdir/$customer/$host.$customer-$plugin_name*"))){ #-f $storable || 
# 	    copy ($storable, "$work_dir/$name$suffix") or LOGDIE "can't get storable file: $!\n";
# 	    make_path "$work_dir/$customer" || LOGDIE "can't create dir $work_dir/$customer.\n";
	    foreach my $file (glob("$munin_dbdir/$customer/$host.$customer-$plugin_name*")){
# 		my ($name,$dir,$suffix) = fileparse($file, qr/\.[^.]*/); 
		my $timestamp = `rrdtool info $file` || LOGDIE "can't run rrdtool\n";
		($timestamp) = grep {m/^last_update/} (split /\n/, $timestamp);
		$timestamp =~ s/^last_update\s*=\s*//;
		$last_timestamp = $timestamp + 0 if $timestamp > $last_timestamp || $last_timestamp == 0;
		DEBUG "Using timestamp $last_timestamp\n";
# 		copy ($file, "$work_dir/$customer/") or LOGDIE "can't copy rrd file $file: $!\n";
# 		symlink($file, "$work_dir/$customer/$name$suffix") or LOGDIE "can't symlink rrd file $file: $!\n";
	    };
# 	    my $storable_hash = eval { Storable::retrieve("$work_dir/$name$suffix"); };
# 	    $storable_hash->{spoolfetch} = $last_timestamp;
# 	    Storable::nstore($storable_hash, "$work_dir/$name$suffix");
# 	}
#     } elsif ($direction == 2) {
# 	dircopy("$work_dir/$customer", "$munin_dbdir/$customer") or LOGDIE "can't copy dir $work_dir/$customer: $!\n";
# 	copy ("$work_dir/$name$suffix", $storable) or LOGDIE "can't copy storable file: $!\n";
## copy datafiles in     $munin_db_fake_dir
# 	if (-f "$work_dir/datafile") {
# 	    open(FILE, "$work_dir/datafile") or LOGDIE "Can't open file for reading: $!\n";
# 	    my @new_lines = <FILE>;
# 	    close(FILE);
# 
# 	    open(FILE, ">>$munin_dbdir/datafile") or LOGDIE "Can't open file for writing: $!\n";
# 	    print FILE "\n";
# 	    foreach (@new_lines) {
# 		print FILE $_;
# 	    }
# 	    close(FILE);
# 	}
#     } else {
# 	LOGDIE "unknown direction = $direction\n";
#     }
    return $last_timestamp ? $last_timestamp : $last_timestamp;
}

sub configPlugins {
    my ($plugin_name, $colums_name) = @_;
    my $plugin_conf_template = "$plugins_conf_dir/$plugin_name.conf";
    if (! -f $plugin_conf_template || ! -s $plugin_conf_template) {
	open(FILE, ">$plugin_conf_template") or LOGDIE "Can't open file $plugin_conf_template for writing: $!\n";
	foreach (keys %$colums_name) {
	    next if $colums_name->{$_}->{name} =~ m/^\s*Date\s*$/i || $colums_name->{$_}->{name} =~ m/^\s*Time\s*$/i;
	    print FILE $colums_name->{$_}->{name}."\n";
	}
	close(FILE);
    }

    my $plugin_conf_file = "$plugins_conf_dir/customers/$customer/$host/$plugin_name.conf";
    if (! -f $plugin_conf_file) {
	make_path "$plugins_conf_dir/customers/$customer/$host" || LOGDIE "can't create dir for plugin conf.\n";
	copy ($plugin_conf_template, $plugin_conf_file) or LOGDIE "can't copy plugin conf file: $!\n";
    }

    my $graph_name;
    open(FILE, $plugin_conf_file) or LOGDIE "Can't open file for reading: $!\n";
    my $section = "Other";
    my @extra_args;
    foreach my $line (<FILE>) {
	$line =~ s/(\n*|\r*)//g;
	if ($line =~ m/^\s*\[(.*?)\]\s*$/){
	    my $name = $1;
	    @extra_args = ();
	    if ($name =~ m/^\s*update_rate\s*=\s*([0-9]+)\s*$/i) {
		$rate = $1 if defined $1 and $1 > 0;
		DEBUG "Update rate changhed to $rate\n";
	    } elsif ($name =~ m/:/) {
		DEBUG "Found extra config.\n";
		my @vars = split /:/, $name;
		$section = shift @vars;
		foreach (@vars) {
		    DEBUG "Found config $_.\n";
		    push @extra_args, $_;
		}
	    } else {
		$section = $name;
	    }
	} else {
	    $graph_name->{$line}->{section} = $section;
	    push @{$graph_name->{$line}->{args}}, @extra_args if scalar @extra_args;
	}
    }
    close(FILE);
    return $graph_name;
}

sub connectDB {
    $db_h = DBI->connect("DBI:mysql:$config->{db_config}->{db_database}:10.0.0.99:3306", $config->{db_config}->{db_user}, $config->{db_config}->{db_pass},
	{ ShowErrorStatement => 1,
          AutoCommit => 1,
          RaiseError => 1,
          mysql_use_result => 1,
          mysql_enable_utf8 => 1,
	  mysql_auto_reconnect => 1,
          PrintError => 1, }) || die "Could not connect to database: $DBI::errstr";
    return $db_h;
}

sub fixInput{
    my $input = shift;
    my ($host_id, $stats_table, $plugin_name, $customer_guess) = $input =~ m/^(.*)?\+((.*)?statistics?_(.*))$/;
    return ($host_id, $stats_table, $plugin_name, $customer_guess);
}

sub run {
    my $input = shift;
    # $plugin_name MUST contain only 0-9a-z_
my ($host_id, $stats_table, $plugin_name, $customer_guess) = fixInput($input);
# print Dumper("orig=".$input,"host=".$host_id,"cust=".$customer_guess,"plugin=".$plugin_name,"table=".$stats_table);exit 1000;
# my $stats_table = $plugin_name."statistics_".$customer_guess;
# my $not_done = 1;
# while ($not_done){
    connectDB;
#     foreach my $stats_table (keys %$tables) {
	initVars($host_id, $plugin_name);
	LOGDIE "$input - strange customers: $customer_guess $customer\n" if lc($customer_guess) ne lc($customer);
	INFO "$input - Start munin work using table $stats_table with host id=$host_id, name=$hostname\n";
# print Dumper($host_id, $stats_table, $customer_guess, $plugin_name);exit 1000;
# next if $stats_table =~ m/rts/i;
	my $colums_name = getColumns($stats_table);
# 	print Dumper(scalar (keys %$colums_name));exit 1000;
# 	my ($plugin_name) = $stats_table =~ m/^([^_]+)/;
# 	my $plugin_name = "test_$type\_$host\_$customer\_mind";  # MUST contain only 0-9a-z_
# 	my $plugin_name = "$type";  # MUST contain only 0-9a-z_

	my $graph_name = configPlugins($plugin_name, $colums_name);
	my $from_time = copyOldFiles($plugin_name);
	INFO "$input - Start time is $from_time\n";
# 	exit 1000;
	writeConfFiles();

	DEBUG "$input - Getting lines from $stats_table with host_id=$host_id, timestamp=$from_time\n";
	my ($total_rows) = $db_h->selectrow_array("SELECT count(*) FROM $stats_table WHERE host_id=$host_id and timestamp>=$from_time order by timestamp asc");
	my $batch_nr_rows = sprintf("%.0f", 30000/(scalar (keys %$colums_name)))+2;
	my $sth = $db_h->prepare("SELECT * FROM $stats_table WHERE host_id=$host_id and timestamp>? order by timestamp limit $batch_nr_rows");
# 	print Dumper($sth->{NUM_OF_FIELDS});
	$sth->execute(($from_time)) || LOGDIE "Error $DBI::errstr\n";
# 	print Dumper($sth->{NUM_OF_FIELDS});

	my $crt_rows = 0;
	


	DEBUG "$input - Retrieving rows in batches of $batch_nr_rows\n";
# eval{
	while (my $aref = $sth->fetchall_arrayref({}) ){
	    LOGDIE "No rows\n" if ! (scalar @$aref);
	    unlink glob ("$work_dir/munin-daemon.$plugin_name*");
    
	    DEBUG "$input - got nr rows : ".(scalar @$aref).".\n";
	    foreach my $row (@$aref){
		my @output_rows = (
    # 		"graph_title $stats_table",
    # 		"graph_vlabel eceva pt vlabel",
		    "graph_scale no",
		    "graph_category $plugin_name",
		    "graph_info $stats_table",
		    "update_rate $rate",
		    "graph_data_size custom 1y, 1h for 2y, 1d for 5y",
    # 		"hostname $hostname",
		    );
		my $q;
#     print Dumper($row);
		foreach my $md5 (keys %$row) {
		    next if $md5 =~ m/^(id|host_id|file_id|timestamp|date|time)$/i || $colums_name->{$md5}->{name} =~ m/^(date|time|DBQueues)$/i;
# LOGDIE "flockers $md5:\n".Dumper($row) if ! defined $row->{$md5};
		    next if ! defined $row->{$md5};
		    my $name = $colums_name->{$md5}->{name};
		    my $val = $row->{$md5};
		    my $section = $graph_name->{$name}->{section};
		    if (! defined $q->{$section} ) {
			push @{ $q->{$section} }, @{ $graph_name->{$name}->{args} } if defined $graph_name->{$name}->{args};
			push @{ $q->{$section} }, (@output_rows, "graph_title $section");
		    }
		    push @{ $q->{$section} }, (
    # 		    "multigraph $graph_name->{$name}",
			"$md5.label $name",
			"$md5.info $name",
			"$md5.value $val",
		    );
		}

		foreach my $key (keys %$q) {
		    my $plugin_name_ok = $key;
		    $plugin_name_ok =~ s/[^a-z0-9_]/_/gi;
# print "$row->{date} $row->{time} ".Dumper($q->{$_})."\n";
# print Dumper($q->{$key}, $plugin_name."_".$plugin_name_ok, $q);
		    my $q = $spoolwriter->write($row->{timestamp}, $plugin_name."_".$plugin_name_ok, $q->{$key}) ;
		}
		$from_time = $row->{timestamp};
		
	    }
# 	    if ($crt_rows%(300*20))
	    DEBUG "$input - Running munin-update for $hostname in $stats_table\n";
	    system("/opt/munin/lib/munin-update", "--config_file=$conf_file") == 0 or return EXIT_MUNIN_ERROR; #, "--host", "$hostname", "--debug"
	    $sth->execute($from_time) || LOGDIE "Error $DBI::errstr\n";
	    if ($crt_rows == 0) {
		copy ("$work_dir/datafile", "$munin_db_fake_dir/datafile.$plugin_name") or LOGDIE "can't copy datafile file $work_dir/datafile to $munin_db_fake_dir/datafile.$plugin_name: $!\n";
	    }
	    $crt_rows += scalar @$aref;
	    INFO "$input - Total rows is $total_rows, already done is $crt_rows ($input).\n";
# 	    if ($batch_nr_rows==1){
		## on 3000 rows iostat can generate a 150MB file. also mysql dies if we use too many rows
# 	    }
# 	    system("/opt/munin/lib/munin-limits"); #, "--debug"
# 	    system("/opt/munin/lib/munin-html"); #, "--debug"
	}
# };
# my $exit_status = $@;
# if ($exit_status) {
#   WARN "$input - Got error on updating munin data: $exit_status\n";
#   if ($exit_status =~ m/Lost connection to MySQL server during query/) {
#     WARN "$input - SQL error for $stats_table from host id=$host_id, name=$hostname, retry.\n";
#   } else {
#     print Dumper($exit_status);
#     return EXIT_MUNIN_ERROR;
#   }
# } else {
#   $not_done = 0;
# }
# return EXIT_MUNIN_ERROR if ! -f "$work_dir/datafile";
# print Dumper("$work_dir/datafile", "$munin_db_fake_dir/datafile.$plugin_name");
 
# 	copyOldFiles(2);
	rmtree ($work_dir);
# 	system("/opt/munin/lib/munin-update") == 0 or return EXIT_MUNIN_ERROR;
# 	system("/opt/munin/lib/munin-limits") == 0 or return EXIT_MUNIN_ERROR;
# 	system("/opt/munin/lib/munin-html") == 0 or return EXIT_MUNIN_ERROR;
	INFO "$input - Done munin update $stats_table from host id=$host_id, name=$hostname\n";
#     }
# };
    return EXIT_MUNIN_SUCCESS;
}

sub finishedWork {
    my ( $input, $new_status, $old_status) = @_;
    my ($host_id, $stats_table, $plugin_name, $customer_guess) = fixInput($input);
    DEBUG "$input - Done munin: Updating all files from hostid=$host_id with status $new_status\n";
    my $sth = $db_h->do("update $config->{db_config}->{collected_file_table} set status=$new_status WHERE status=$old_status and host_id=$host_id") || die "Error $DBI::errstr\n";
    foreach (MindCommons::find_files_recursively("$Munin::Common::Defaults::MUNIN_SPOOLDIR/faker/")){
	next if $_ !~ m/datafile/;
	print "$_\n";
    }
    return $new_status;
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
