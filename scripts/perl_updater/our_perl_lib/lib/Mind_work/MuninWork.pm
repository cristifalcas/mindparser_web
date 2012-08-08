package MuninWork;
## needs from rc.local
## disable graph generation
use warnings;
use strict;

use Munin::Node::SpoolWriter;
use Munin::Common::Defaults;
# my $worker_timeout = $worker->{node}->{configref}->{worker_timeout} ? int ($worker->{node}->{configref}->{worker_timeout}) : $self->{worker_timeout};
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
 use File::Path qw(make_path);

use Mind_work::SqlWork;
use Mind_work::MindCommons;
my $config = MindCommons::xmlfile_to_hash("config.xml");
# my $db_database = $config->{db_config}->{db_database};
# my $db_user = $config->{db_config}->{db_user};
# my $db_pass = $config->{db_config}->{db_pass};
my $cust_table = $config->{db_config}->{cust_table};
my $host_table = $config->{db_config}->{host_table};
my $collected_file_table = $config->{db_config}->{collected_file_table};
my $md5_names_table = $config->{db_config}->{md5_names_table};
my $stats_template_table = $config->{db_config}->{stats_template_table};
my $db_h;
my $rate = 30;

my $SPOOLDIR = "$Munin::Common::Defaults::MUNIN_SPOOLDIR";
my $intervalsize = 86400;
my $retaincount = 1;

my ($plugin_rate, $plugin_name, $spoolwriter);

sub setGlobalUpdateRate {
    use Fcntl;
    use DB_File;
    my %hash;
    tie (%hash, 'DB_File', "$SPOOLDIR/plugin_rates", O_RDWR|O_CREAT, 0666) or die "$!";
#     print Dumper(%hash);
    $hash{$plugin_name} = $plugin_rate;
    untie(%hash);
}

sub setVars {
    my ($host) = @_;
make_path "$SPOOLDIR/$host/" || die "can't create spoo, dir\n";
open(MYOUTFILE, ">/media/share/Documentation/cfalcas/q/parse_logs/munin.conf.d/$host.conf") ||die "can't open file /media/share/Documentation/cfalcas/q/parse_logs/munin.conf.d/$host.conf: $!\n";
print MYOUTFILE "[$host]\n\tupdate yes\n\t#worker_timeout 5\n\taddress ssh://munin\@localhost /opt/munin/lib/munin-async --spoolfetch --spooldir $SPOOLDIR/$host --cleanup";
close(MYOUTFILE);
    $spoolwriter = Munin::Node::SpoolWriter->new(
	spooldir => "$SPOOLDIR/$host/",
	interval_size => $intervalsize,
	interval_keep => $retaincount,
	hostname  => $host,
    );
# use Munin::Node::SpoolReader;
# my $spoolreader = Munin::Node::SpoolReader->new( spooldir => "$SPOOLDIR/$host/",);
# print $spoolreader->fetch($last_epoch);
# print Dumper($spoolreader);
}

sub getCustomerName {
    my $host_id = shift;
    return $db_h->selectrow_array("select c.name from hosts h, customers c where h.customer_id=c.id and h.id=$host_id");
}

sub getHostName {
    my $host_id = shift;
    return $db_h->selectrow_array("select name from hosts where id=$host_id");
}

sub getFileInfo {
}

sub run {
    my ($dbh, $host_id, $files) = @_;
    $db_h = $dbh->getDBI_handler();

    ($host_id, my $stats_table) = $host_id=~ m/^(.+):(.+)$/;
    my ($type) = $stats_table =~ m/^([^_]+)/;
    my $customer = getCustomerName($host_id);
    my $host = getHostName($host_id);
# next if $customer ne "Alon";
    setVars("$host.$customer");

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
    
	use Storable;
	my $state_file = sprintf ('%s/state-%s.storable', "$Munin::Common::Defaults::MUNIN_DBDIR", "$customer-$host.$customer");
	my $storable = eval { Storable::retrieve($state_file); };
	my $last_update = $storable->{spoolfetch} || ($db_h->selectrow_array("select min(timestamp) from $stats_table WHERE host_id=$host_id")||0);
	my $from_time = $last_update - $rate;
	my $to_time = $from_time + $intervalsize;
    $db_h->{'mysql_use_result'}=1;
    $sth = $db_h->prepare("SELECT * FROM $stats_table WHERE host_id=$host_id and timestamp>=$from_time and timestamp<=$to_time and file_id in (". (join ",", @$files).") order by timestamp");
    $sth->execute() || die "Error $DBI::errstr\n";
# my $q=1;
    while (my $aref = $sth->fetchall_arrayref({}, 4000) ){
	foreach my $row (@$aref){
	my $output_rows = [
	    "graph_title $stats_table",
	    "graph_vlabel entropy (bytes)_doco",
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
# 		    "$md5.update_rate $rate",
		);
	    }
	    $plugin_rate = $rate;
	    $plugin_name = "test_$host\_$customer\_mind";  # does not contain spaces
# 	    push $output_rows, ("$md5.update_rate $rate",);
# 	    print Dumper($row->{timestamp});
	    $spoolwriter->write($row->{timestamp}, $plugin_name, $output_rows);
# $spoolwriter->write(1344181683+$q*200, $plugin_name, $output_rows);$q++;
	}
	print "got nr rows : ".(scalar @$aref)."\n";
	return;
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
