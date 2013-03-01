package MuninWork;
## needs from rc.local
## disable graph generation in  /opt/munin/bin/munin-cron
# /usr/share/perl5/vendor_perl/Munin/Master/ProcessManager.pm : accept_timeout  => 2,
## update timeouts
# /usr/share/perl5/vendor_perl/Munin/Master/ProcessManager.pm
# /usr/share/perl5/vendor_perl/Munin/Master/Config.pm

use warnings;
use strict;
$| = 1;

use Munin::Node::SpoolWriter;
use Munin::Common::Defaults;
use Munin::Master::Utils;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use File::Path qw(make_path remove_tree);
use File::Copy;
use Cwd 'abs_path';
use File::Basename;

use Log::Log4perl qw(:easy);
use Definitions ':all';

use Mind_work::MindCommons;
my $config_xml = MindCommons::xmlfile_to_hash("config.xml");
my $script_path = (fileparse(abs_path($0), qr/\.[^.]*/))[1]."";
my $spoolwriter;
# "$Munin::Common::Defaults::MUNIN_CONFDIR"
# "$Munin::Common::Defaults::MUNIN_SPOOLDIR"
# "$Munin::Common::Defaults::MUNIN_DBDIR"

sub initVars {
    my ($input, $dbh) = @_;

    my $munin_config = munin_readconfig_base("$Munin::Common::Defaults::MUNIN_CONFDIR/munin.conf");
    $input->{munin_dbdir} = $munin_config->{dbdir};
    $input->{munin_includedir} = $munin_config->{includedir};

    $input->{cust_name} =  $dbh->get_customer_name_from_host_id($input->{host_id});
    $input->{host_name} =  $dbh->get_host_name($input->{host_id});

    $input->{config} = $dbh->getPluginConf($input->{id});
    $input->{columns_md5} = $dbh->get_names_from_md5( $dbh->getColumnList($input->{inserted_in_tablename}) );

    $input->{work_dir} = "$config_xml->{dir_paths}->{filetmp_dir}/munin_work/$input->{cust_name}_$input->{host_name}_$input->{plugin_name}/";
    $input->{tmp_dbdir} = "$input->{work_dir}/tmp_dbdir/";
    $input->{tmp_spooldir} = "$input->{work_dir}/tmp_spooldir/";
    $input->{tmp_conf_file} = "$input->{tmp_dbdir}/$input->{cust_name}_$input->{host_name}.conf";

    return $input;
}

sub writeTempMuninConf {
    my $input = shift;
    open(MYOUTFILE, ">$input->{tmp_conf_file}") || LOGDIE "can't open file $input->{tmp_conf_file}: $!\n";
    print MYOUTFILE "rundir $input->{tmp_dbdir}
dbdir $input->{tmp_dbdir}

[$input->{host_name}.$input->{cust_name}]
      update yes
      address ssh://munin\@localhost /usr/share/munin/munin-async --spoolfetch --spooldir $input->{tmp_dbdir} --cleanup\n";
    close(MYOUTFILE);
}

sub lastTimeFromRRDFiles {
    my ($input, $group) = @_;
#     my $last_timestamp = 0;
    $group  =~ s/[^a-z0-9_]/_/ig;
    $group = "_$group" if $group ne '';
    ## one spawn because it's expensive
    my $timestamp = `for file in \$(ls "$input->{munin_dbdir}/$input->{cust_name}/$input->{host_name}.$input->{cust_name}-$input->{plugin_name}$group-"*  2> /dev/null | grep -v $group_default_filler);do rrdtool info  \$file | grep last_update;done | sort | uniq`;
    LOGDIE "Wrong output:\n".Dumper($group, $timestamp, ``) if ! defined $timestamp || scalar (split /\n/, $timestamp) > 1;
    $timestamp =~ s/(^last_update\s*=\s*)|(\s*$)//g;
    $timestamp = 0 if $timestamp =~ m/^\s*$/;
#     ERROR Dumper($timestamp);

#     foreach my $file (glob("$input->{munin_dbdir}/$input->{cust_name}/$input->{host_name}.$input->{cust_name}-$input->{plugin_name}*")){
# 	next if ! -s $file;
# 	next if $file !~ m/$group/i;
# 	my $timestamp = `rrdtool info "$file"` || LOGDIE "can't run rrdtool info $file`\n";
# 	($timestamp) = grep {m/^last_update/} (split /\n/, $timestamp);
# 	$timestamp =~ s/^last_update\s*=\s*//;
# 	if ($timestamp > $last_timestamp || $last_timestamp == 0) {
# 	    $last_timestamp = $timestamp + 0;
# 	}
# 	DEBUG "Using timestamp $last_timestamp from $file\n";
#     };
    return $timestamp;
}

sub getHeaderMunin {
    my ($input, $group) = @_;
    my @array = (
# 		"graph_title title.$input->{plugin_name}_$group",
# 		"graph_vlabel eceva pt vlabel",
		"graph_scale no",
		"graph_category $input->{plugin_name}",
# 		"graph_info $input->{inserted_in_tablename}",
		"update_rate $input->{update_rate}",
		"graph_data_size custom 1y, 1h for 2y, 1d for 5y",
# 		"hostname $input->{host_name}.$input->{cust_name}.fake",
		);
    return @array;
}

sub write_to_spool {
    my ($input, $row, $group, $timestamp) = @_;

#     foreach my $section (keys %$q) {
# 	delete $input->{config}->{$section}->{__munin_extra_info};
# 	my $hash = $input->{config}->{$section};
# 	my $nr_values = scalar keys %$hash;
# 	if ($nr_values > 1){
# 	    push @{ $q->{$section} }, ("wrongdata_all.cdef ".(join ",", keys %$hash).(",+" x ($nr_values-1)) );
# 	} else {
# 	    push @{ $q->{$section} }, ("wrongdata_all.cdef ".(keys %$hash)[0] );
# 	}
# 	push @{ $q->{$section} }, (
# 	    "wrongdata_all.graph no",
# 	    "wrongdata_all.label wrongdata_all",
# 	    "wrongdata.cdef wrongdata_all,UN,INF,UNKN,IF", #".(shift @{$all_md5_per_section->{$section}})."
# 	    "wrongdata.draw AREA",
# 	    "wrongdata.colour DEDEDE",
# 	    "wrongdata.label Missing data",
# 	);
# 
# 	my $name_ok = "$input->{plugin_name}_$section";
# 	$name_ok .= "_$group" if defined $group && $group  !~ m/^\s*$/;
# 	$name_ok =~ s/[^a-z0-9_]/_/gi;
# 
# 	$spoolwriter->write($timestamp, $name_ok, $q->{$section}) ;
#     }

    my $columns_header_string = join "\t", @$columns_header;
    my @header = getHeaderMunin($input, $group);
#     $group =~ s/^$stats_default_info->{$input->{plugin_name}}->{group_by}->[0]\_//;
    my $q = [@header, "graph_title $group"];
# INFO Dumper($row);
    foreach my $md5 (keys %$row) {
	next if $columns_header_string =~ m/$md5/i || ! defined $row->{$md5};
# LOGDIE Dumper($input->{columns_md5}, $md5);
	my $name = $input->{columns_md5}->{$md5};#."_$group";
	my $val = $row->{$md5};
	push @$q, ("$md5.label $name", "$md5.value $val");
    }
    my $name_ok = "$input->{plugin_name}";
    $name_ok .= "_$group" if defined $group && $group  !~ m/^\s*$/;
    $name_ok =~ s/[^a-z0-9_]/_/gi;

    $spoolwriter->write($timestamp, $name_ok, $q) ;

    return $timestamp;
}

# sub make_munin_info {
#     my ($row, $dbh, $input, $group) = @_;
# 
# #     my $md5_to_section;
# #     my $config = $input->{config};
# #     foreach my $section (keys %$config){
# # 	my $md5s = $config->{$section};
# # 	foreach my $md5 (keys %$md5s){
# # 	    $md5_to_section->{$md5} = $section if $md5 ne "__munin_extra_info";
# # 	}
# #     }
# 
#     my $columns_header_string = join "\t", @$columns_header;
#     my @header = getHeaderMunin($input, $group);
# #     $group =~ s/^$stats_default_info->{$input->{plugin_name}}->{group_by}->[0]\_//;
#     my $q = [@header, "graph_title $group"];
#     foreach my $md5 (keys %$row) {
# 	next if $columns_header_string =~ m/$md5/i || ! defined $row->{$md5};
# 	my $name = $input->{columns_md5}->{$md5};#."_$group";
# 	my $val = $row->{$md5};
# # 	my $section = $md5_to_section->{$md5};
# 	## push once only graph_title, header and args for title
# # 	if (! defined $q->{$section} ) {
# # 	    push @{ $q->{$section} }, @{ $config->{$section}->{__munin_extra_info} } if defined $config->{$section}->{__munin_extra_info};
# # 	    push @{ $q->{$section} }, (@header, "graph_title $section\_$group");
# # 	}
# # 	push @{ $q->{$section} }, (
# # 	    "$md5.label $name",
# # 	    "$md5.info $name",
# # 	    "$md5.value $val",
# # 	);
# 	push @$q, ("$md5.label $name", "$md5.value $val");
# # 		push @{ $all_md5_per_section->{$section} }, ($md5);
#     }
#     return $q;
# }

sub cleanWorkDir {
    my $input = shift;

    if (-d $input->{tmp_dbdir}) {
      remove_tree ($input->{tmp_dbdir}) || LOGDIE "can't remove tmp dbdir $input->{tmp_dbdir}: $!\n";
    }

    make_path $input->{tmp_dbdir} || LOGDIE "can't create dbdir dir $input->{tmp_dbdir}: $!\n";
    make_path $input->{tmp_spooldir} || LOGDIE "can't create tmp spooldir dir $input->{tmp_spooldir}: $!\n";
    make_path "$input->{munin_dbdir}/$input->{cust_name}/" || LOGDIE "can't create dir $input->{munin_dbdir}/$input->{cust_name}/: $!\n";
    system("ln", "-s", "$input->{munin_dbdir}/$input->{cust_name}/", "$input->{tmp_dbdir}/") == 0 || LOGDIE "can't symlink rrd dir: $!\n";
    writeTempMuninConf($input);

    $spoolwriter = Munin::Node::SpoolWriter->new(
	spooldir => $input->{tmp_dbdir},
    ## munin will not update previous day on day change at all otherwise
	interval_size => 86400 * 365,
	interval_keep => 365,
	hostname  => "$input->{host_name}.$input->{cust_name}",
    );
}


sub addRowsToRRD {
    my ($input, $group, $nr_rows_from_db) = @_;
# my $q = rand(10);print "pif = $input, sleep $q\n";sleep $q;return;
#     cleanWorkDir($input);
#     my $from_time = $input->{from_time};
    my $from_time = lastTimeFromRRDFiles($input, $group);
    $nr_rows_from_db = sprintf("%.0f", 100000/(scalar keys %{ $input->{columns_md5} }))+2 if ! defined $nr_rows_from_db;

    my $dbh = new SqlWork();
    my $db_h = $dbh->getDBI_handler();
    my $total_rows = $db_h->selectrow_array("SELECT count(*) FROM $input->{inserted_in_tablename} 
	  WHERE host_id=$input->{host_id} 
		and group_by='$group' 
		and timestamp>=$from_time 
	  order by timestamp asc");
    INFO "Getting max $nr_rows_from_db rows out of $total_rows from $input->{inserted_in_tablename} ($group) with host $input->{host_name}, timestamp=$from_time.\n";

    my $sth = $db_h->prepare("SELECT * FROM $input->{inserted_in_tablename} 
	  WHERE host_id=$input->{host_id} 
		and group_by='$group' 
		and timestamp > ? 
	  order by timestamp asc limit ?");
#     my $last_inserted = $db_h->selectrow_arrayref("SELECT IFNULL(max(timestamp),0) FROM $input->{inserted_in_tablename} WHERE host_id=$input->{host_id} and group_by='$group'");
#     $sth->execute($prev_last_inserted->[0], 1) || LOGDIE "Error $DBI::errstr\n";
#     $sth->execute($from_time, $from_time ? $nr_rows_from_db : 1) || LOGDIE "Error $DBI::errstr\n";
    $sth->execute($from_time, $nr_rows_from_db) || LOGDIE "Error $DBI::errstr\n";

    $group =~ s/[^a-z0-9]/_/gi;
    my $crt_rows = 0;
    while (my $aref = $sth->fetchall_arrayref({}) ){
	$crt_rows += scalar @$aref;
	INFO "\tgot nr rows : $crt_rows out of $total_rows.\n";
	last if ! (scalar @$aref);
# 	my $crt_last = pop @$aref;
# 	if ($last_inserted->[0] == $crt_last) {
# 	}
	foreach my $row (@$aref){
	    next if ! scalar keys %$row;
# 	    my $q = make_munin_info($row, $dbh, $input, $group);
# 	    write_to_spool($q, $row->{timestamp}, $input, $group);
# 	    $from_time = $row->{timestamp};
# $hash_filler->{$group_default_filler}, $group_default_filler
	    if ($nr_rows_from_db == 1){
		$row->{$group_default_filler} = 'NAN';
		$input->{columns_md5}->{$group_default_filler} = $group_default_filler;
	    }
	    $from_time = write_to_spool($input, $row, $group, $row->{timestamp});
	}
	my $logger = get_logger();my $munin_debug = $logger->is_debug() || 0 ? "--debug" : "";
	DEBUG "\tRunning munin-update for $input->{host_name}.$input->{cust_name} in $input->{inserted_in_tablename}\n";
	system("/usr/share/munin/munin-update", "--config_file=$input->{tmp_conf_file}", "--nofork", $munin_debug) == 0 or LOGDIE "Munin ended with errors.\n";
	if ($nr_rows_from_db == 1) {
# 	    my $dest_dir = "$config_xml->{dir_paths}->{generic_spooldir}/$input->{host_name}.$input->{cust_name}/";
	    my $dest_dir = $input->{tmp_spooldir};
# 	    make_path $dest_dir || LOGDIE "can't create dir $dest_dir: $!\n";
# 	    unlink glob("$dest_dir/munin-daemon.$input->{plugin_name}_$group*");
	    foreach (glob("$input->{tmp_dbdir}/munin-daemon.$input->{plugin_name}*")) {
		my ($name, $dir, $suffix) = fileparse($_, qr/\.[^.]*/);
		DEBUG "Copy file $_ to $dest_dir for real munin.\n";
		copy ($_, "$dest_dir/$name$suffix") || LOGDIE "can't copy file $_ to $dest_dir/$name$suffix: $!\n";
		return;
	    }
	    LOGDIE "We shouldn't get here.\n";
# 	    unlink "$input->{munin_dbdir}/state-$input->{cust_name}-$input->{host_name}.$input->{cust_name}.storable";
# 	    copy ("$input->{tmp_dbdir}/datafile", "$dest_dir/datafile.$group") || LOGDIE "can't copy file $input->{tmp_dbdir}/datafile: $!\n";
	}
	$sth->execute($from_time, $nr_rows_from_db) || LOGDIE "Error $DBI::errstr\n";
	DEBUG "\tTotal rows is $total_rows, already done is $crt_rows.\n";
    }
    $dbh->disconnect;
    INFO "Done $total_rows rows update $input->{inserted_in_tablename} from host=$input->{host_name}\n";
}

sub all_groups_fork {
    my ($input, $groups, $first) = @_;
    foreach my $group (sort @$groups) {
	my $pid = fork();
	if ($pid == 0) {
	    INFO "Start pid $$\n";
	    $input->{tmp_dbdir} .= "_$$/";
	    $input->{tmp_conf_file} .= ".$$";
	    cleanWorkDir($input);
# 	    $dbh->cloneForFork();
	    addRowsToRRD($input, $group->[0], $first) ;
	    exit 0;
	}
    }
}

sub wait_all_forks {
    use POSIX ":sys_wait_h";
    use Time::HiRes qw(usleep);
    while (1) {
	my $pid = waitpid(-1, WNOHANG);
	last if $pid == -1;
	my $exit_status = $? >> 8;
	INFO "Stopped pid $pid with status=$exit_status.\n" if ($pid > 0);
	usleep 100000;
    }
}

sub copy_async_data {
    my $input = shift;
    my $dest_dir = "$config_xml->{dir_paths}->{generic_spooldir}/$input->{host_name}.$input->{cust_name}/";
    make_path $dest_dir || LOGDIE "can't create dir $dest_dir: $!\n";
    unlink glob("$dest_dir/munin-daemon.$input->{plugin_name}_*");
    foreach (glob("$input->{tmp_spooldir}/munin-daemon.$input->{plugin_name}*")) {
	my ($name, $dir, $suffix) = fileparse($_, qr/\.[^.]*/);
	DEBUG "Copy file $_ to $dest_dir for real munin.\n";
	copy ($_, "$dest_dir/$name$suffix") || LOGDIE "can't copy file $_ to $dest_dir/$name$suffix: $!\n";
    }
}

sub run {
    my ($data, $dbh) = @_;
    INFO "Start munin work for $data->{id}.\n";
    $data = initVars($data, $dbh);
    return ERRORS_START if ($data->{update_rate} <= 0);

    remove_tree $data->{work_dir} if -d $data->{work_dir};
    make_path $data->{work_dir} || LOGDIE "can't create dbdir dir $data->{work_dir}: $!\n";

    ## one for the money
#     addRowsToRRD($data, $_->[0], 1) foreach (@{ $dbh->getGroupsForPlugin($data->{host_id}) });
    all_groups_fork($data, $dbh->getGroupsForPlugin($data->{host_id}), 1);
    wait_all_forks();
    copy_async_data($data);

#     my $dest_dir = "$config_xml->{dir_paths}->{generic_spooldir}/$data->{host_name}.$data->{cust_name}/";
#     make_path $dest_dir || LOGDIE "can't create dir $dest_dir: $!\n";
#     unlink glob("$dest_dir/munin-daemon.$data->{plugin_name}_*");
#     foreach (glob("$data->{tmp_spooldir}/munin-daemon.$data->{plugin_name}*")) {
# 	my ($name, $dir, $suffix) = fileparse($_, qr/\.[^.]*/);
# 	DEBUG "Copy file $_ to $dest_dir for real munin.\n";
# 	copy ($_, "$dest_dir/$name$suffix") || LOGDIE "can't copy file $_ to $dest_dir/$name$suffix: $!\n";
#     }
# return 0;
    ## two for the show
    all_groups_fork($data, $dbh->getGroupsForPlugin($data->{host_id}));
    wait_all_forks();
#     foreach (sort @{ $dbh->getGroupsForPlugin($data->{host_id}) }) {
# 	my $pid = fork();
# 	if ($pid == 0) {
# 	    INFO "Start pid $$\n";
# 	    $data->{tmp_dbdir} .= "_$$/";
# 	    $data->{tmp_conf_file} .= ".$$";
# # 	    $dbh->cloneForFork();
# 	    addRowsToRRD($data, $_->[0]) ;
# 	    exit 0;
# 	}
#     }

#     use POSIX ":sys_wait_h";
#     while (1) {
# 	my $pid = waitpid(-1, WNOHANG);
# 	last if $pid == -1;
# 	my $exit_status = $? >> 8;
# 	INFO "child $pid died with status=$exit_status: reapead.\n" if ($pid > 0);
# 	sleep 1;
#     }

    $dbh->setDoneNeedsUpdate ($data->{id});
    INFO "Done munin work for $data->{id}.\n";
    return 0;
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

return 1;
