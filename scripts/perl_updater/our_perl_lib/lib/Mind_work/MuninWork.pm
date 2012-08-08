package MuninWork;
## needs from rc.local
## disable graph generation
use warnings;
use strict;

use Munin::Node::SpoolWriter;
use Munin::Common::Defaults;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use File::Path qw(make_path);

use Mind_work::SqlWork;
use Mind_work::MindCommons;
# my $db_database, $db_user, $db_pass;
my ($cust_table, $host_table, $collected_file_table, $md5_names_table, $stats_template_table, $db_h);
my ($plugin_rate, $plugin_name, $spoolwriter, $munin_dbdir, $intervalsize, $retaincount, $rate);
my ($plugins_conf_dir, $munin_conf_dir);

sub setGlobalUpdateRate {
    my $spool_dir = shift;
    use Fcntl;
    use DB_File;
    my %hash;
    tie (%hash, 'DB_File', "$spool_dir/plugin_rates", O_RDWR|O_CREAT, 0666) or die "$!";
#     print Dumper(%hash);
    $hash{$plugin_name} = $plugin_rate;
    untie(%hash);
}

sub initVars {
    my ($host, $customer) = @_;
    use Cwd 'abs_path';
    use File::Basename;
    my $config = MindCommons::xmlfile_to_hash("config.xml");
#     my $db_database = $config->{db_config}->{db_database};
#     my $db_user = $config->{db_config}->{db_user};
#     my $db_pass = $config->{db_config}->{db_pass};
    $cust_table = $config->{db_config}->{cust_table};
    $host_table = $config->{db_config}->{host_table};
    $collected_file_table = $config->{db_config}->{collected_file_table};
    $md5_names_table = $config->{db_config}->{md5_names_table};
    $stats_template_table = $config->{db_config}->{stats_template_table};

    $plugins_conf_dir = $config->{dir_paths}->{plugins_conf_dir_postfix};
    
    $munin_dbdir = "$Munin::Common::Defaults::MUNIN_DBDIR";
    $intervalsize = 86400;
    $retaincount = 1;
    $rate = 30;
    
    my $filetmp_dir = $config->{dir_paths}->{filetmp_dir};
#     my $spool_dir = "$Munin::Common::Defaults::MUNIN_SPOOLDIR/$customer/$host";
    my $spool_dir = "$filetmp_dir/munin/$customer\_$host/";
    make_path "$spool_dir" || die "can't create spool dir $spool_dir.\n";
    my $script_path = (fileparse(abs_path($0), qr/\.[^.]*/))[1]."";
    $munin_conf_dir = "$script_path/".$config->{dir_paths}->{munin_conf_dir_postfix};

    $spoolwriter = Munin::Node::SpoolWriter->new(
	spooldir => $spool_dir,
	interval_size => $intervalsize,
	interval_keep => $retaincount,
	hostname  => "$host.$customer",
    );
    
    open(MYOUTFILE, ">$munin_conf_dir/$customer\_$host.con") ||die "can't open file $munin_conf_dir/$customer\_$host.con: $!\n";
    print MYOUTFILE "
rundir $spool_dir
dbdir $spool_dir

[$host_name]
      update yes
      #worker_timeout 5
      address ssh://munin\@localhost /opt/munin/lib/munin-async --spoolfetch --spooldir $spool_dir --cleanup";
    close(MYOUTFILE);
}

sub getCustomerName {
    my $host_id = shift;
    return $db_h->selectrow_array("select c.name from hosts h, customers c where h.customer_id=c.id and h.id=$host_id");
}

sub getHostName {
    my $host_id = shift;
    return $db_h->selectrow_array("select name from hosts where id=$host_id");
}

sub getLastTimestamp{
    my ($host, $customer) = @_;
    use Storable;
    my $state_file = sprintf ('%s/state-%s.storable', $munin_dbdir, "$customer-$host.$customer");
    my $storable = eval { Storable::retrieve($state_file); };
    return $storable->{spoolfetch};
}


sub run {
    my ($dbh, $host_id, $files) = @_;
    $db_h = $dbh->getDBI_handler();

    ($host_id, my $stats_table) = $host_id=~ m/^(.+):(.+)$/;
    my ($type) = $stats_table =~ m/^([^_]+)/;
    my $customer = getCustomerName($host_id);
    my $host = getHostName($host_id);

    initVars($host, $customer);

    my ($sth, @columns);
    eval {
    $sth = $db_h->prepare("SELECT * FROM $stats_table WHERE 1=0");
    $sth->execute;
    @columns = @{$sth->{NAME}}; # or NAME_lc if needed
    $sth->finish;
    };
    return 300 if $@;
    
    $sth = $db_h->prepare("SELECT * FROM $md5_names_table WHERE md5 in (". (join ",", map {$db_h->quote($_)} @columns).")");
    $sth->execute() || die "Error $DBI::errstr\n";
    my $colums_name = $sth->fetchall_hashref('md5');

    my $last_update = getLastTimestamp($host, $customer)  || ($db_h->selectrow_array("select min(timestamp) from $stats_table WHERE host_id=$host_id")||0);
    my $from_time = $last_update - $rate;
    my $to_time = $from_time + $intervalsize;# and timestamp<=$to_time
    $db_h->{'mysql_use_result'} = 1;
    $sth = $db_h->prepare("SELECT * FROM $stats_table WHERE host_id=$host_id and timestamp>=$from_time and file_id in (". (join ",", @$files).") order by timestamp");
    $sth->execute() || die "Error $DBI::errstr\n";
# my $q=1;
    while (my $aref = $sth->fetchall_arrayref({}, 2000) ){
my $first = 0;my $last = 0;
	foreach my $row (@$aref){
	my $output_rows = [
	    "graph_title $stats_table",
# 	    "graph_vlabel eceva pt vlabel",
	    "graph_scale no",
	    "graph_category $customer"."_$type",
	    "graph_info $md5_names_table",
	    "update_rate $rate",
# 	    "timeout 1",
# 	    "host_name $host.$customer",
	    ];
	   foreach my $md5 (keys %$row) {
		next if ! defined $row->{$md5} || $md5 =~ m/^(id|host_id|file_id|timestamp|date|time)$/i;
		my $val = defined $row->{$md5} ? $row->{$md5} : "undef";
		my $name = $md5 =~ m/^(id|host_id|file_id|timestamp|date|time)$/i ? "$md5" : $colums_name->{$md5}->{name};
		push $output_rows, (
		    "$md5.label $name",
		    "$md5.info $name",
		    "$md5.value $val",
		    "$md5.update_rate $rate",
		);
	    }
# 	    $plugin_rate = $rate;
	    $plugin_name = "test_$host\_$customer\_mind";  # does not contain spaces
# 	    push $output_rows, ("$md5.update_rate $rate",);
# 	    print Dumper($row->{timestamp});
	    $spoolwriter->write($row->{timestamp}, $plugin_name, $output_rows);
print "First update at $row->{timestamp}\n" if !$first;
	    $first = $row->{timestamp};
# $spoolwriter->write(1344181683+$q*200, $plugin_name, $output_rows);$q++;
	}
	print "got nr rows : ".(scalar @$aref)."\n";
	return if ! (scalar @$aref);
# 	system("/opt/munin/lib/munin-update", "--host", "$host.$customer", "--config_file=$munin_conf_dir/munin.conf");
	system("/opt/munin/lib/munin-update", "--config_file=$munin_conf_dir/$customer\_$host.conf", "--host", "$host.$customer");
print "Last update at $first\n" if $first;
# 	return;
    }
#     /opt/munin/lib/munin-update --debug --host backend1.Alon --timeout 20 --config_file=/media/share/Documentation/cfalcas/q/parse_logs/munin.conf.d/munin.conf
}

# 

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
