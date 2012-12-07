package ParseStats;

use warnings;
use strict; 
$| = 1;
$SIG{__WARN__} = sub { die @_ };

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Log::Log4perl qw(:easy);
use File::Basename;
use File::Path qw(make_path remove_tree);
use File::Copy;

use Definitions ':all';
my $config = MindCommons::xmlfile_to_hash("config.xml");

# http://www.youtube.com/playlist?list=PLC9BE20B7AE9C9CF3 romanesti comedie
# http://www.youtube.com/playlist?list=PLFACB917F48DBF4C1 comedie
# http://www.youtube.com/playlist?list=PL51081A8426405549 jazz
# http://www.youtube.com/playlist?list=PL1C44B8DCEB524ACF desene 
# http://www.youtube.com/playlist?list=PL54F388AF2FE32D75 carl segan

sub moveFile {
    my ($ret, $data) = @_;
    my $filename = $data->{file_name};
    return if ! -f $filename;
    my $cust_name = $data->{customer_name};
    my $host_name = $data->{host_name};
    my $dir_prefix;
    DEBUG "Returned success $ret for $filename.\n";
    $dir_prefix = "$config->{dir_paths}->{filedone_dir}/$cust_name/$host_name/";
    make_path($dir_prefix);
    my ($name,$dir,$suffix) = fileparse($filename, qr/\.[^.]*/);
    my $new_name = "$dir_prefix/$name"."_".MindCommons::get_random()."$suffix";
    DEBUG "Moving $filename to $new_name\n" if -f $filename;
    move($filename, $new_name) or die "Move file $filename to $new_name failed: $!\n";
}

sub run {
    my ($data, $dbh) = @_;
    INFO "Parsing file $data->{file_name}.\n";
    my $ret;
    if (-f $data->{file_name}) {
	my $parser;
	if ($data->{plugin_info}->{plugin_name} =~ m/^(rts|asc|dialogicopensessions|alon300_ivr|alon3600_ivr)$/) {
	    use Parsers::MindGenericStatistics;
	    $parser = new MindGenericStatistics();
	} elsif ($data->{plugin_info}->{plugin_name} eq "asccoco") {
	    use Parsers::MindGenericStatistics;
	    $parser = new MindGenericStatistics();
	} else {
	    LOGDIE "We don't know how to parse files of type $data->{plugin_info}->{plugin_name} yet.\n";
	}
	$ret = $parser->parse($data, $dbh);
	moveFile($ret, $data);
    } else {
	$ret = EXIT_NO_FILE;
    }
    LOGDIE "Exit status for file $data->{file_name} was $ret\n" if $ret != START_MUNIN && $ret != EXIT_NO_FILE;
    $dbh->updateFileColumns($data->{id}, ['status'], [$ret]);
    $dbh->decreasePluginQueue($data->{plugin_id});

    return 0;
}

return 1;
