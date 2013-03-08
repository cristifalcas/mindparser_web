package GraphWork;

use warnings;
use strict;
$| = 1;

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

sub run {
    my ($data, $dbh) = @_;
    INFO "Start munin work for $data->{id}.\n";
    return 0;
}

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


return 1;

# http://munin-monitoring.org/wiki/faq
# http://munin-monitoring.org/wiki/Combined_examples
# http://munin-monitoring.org/wiki/aggregate_examples
# http://blog.loftninjas.org/2010/04/08/an-evening-with-munin-graph-aggregation/
