package ExtractFiles;

# get files from db
# check type and extract if archive in tmp
# move files back so they can be find again by the watchers
# if file is text, we set it as parsers and update in db

use warnings;
use strict; 
$| = 1;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use File::Copy;
use File::Basename;
use File::Path qw(make_path remove_tree);
use Log::Log4perl qw(:easy);
use Archive::Extract;
use File::LibMagic;
use Definitions ':all';

my $config = MindCommons::xmlfile_to_hash("config.xml");

sub extract_file {
    my ($filename, $flm) = @_;
    INFO "Trying to extract $filename\n";
# use Time::HiRes qw( usleep tv_interval gettimeofday);
# my $t0 = [gettimeofday];

    return EXIT_NO_FILE if ! -f $filename;

#     my $flm = File::LibMagic->new();
    my ($name,$dir,$suffix) = fileparse($filename, qr/\.[^.]*/);
    my $mime_type = $flm->checktype_filename($filename);
# WARN Dumper(tv_interval($t0));

    if ($mime_type eq 'text/plain; charset=us-ascii') {
	if ( $suffix ne ".log"){
	  DEBUG "Rename $filename to log.\n";
	  move($filename, "$dir/$name$suffix\_".MindCommons::get_random.".log") || LOGDIE "Can't rename file $filename: $!\n";
	  return EXIT_NO_FILE; ## file is gone now
      }
      DEBUG "Extract has nothing to do with file $name$suffix.\n";
      return START_PARSERS;
    } elsif ($mime_type eq 'application/zip; charset=binary') {
	if ($suffix ne ".zip") {
	  DEBUG "Rename $filename to zip.\n";
	  move($filename,"$dir/$name$suffix\_".MindCommons::get_random.".zip") || LOGDIE "Can't rename file $filename: $!\n";
	  return EXIT_NO_FILE;
	}
    } elsif ($mime_type eq 'application/x-gzip; charset=binary') {
	if ($suffix ne ".gz" && $suffix ne ".tgz") {
	  DEBUG "Rename $filename to gz.\n";
	  move($filename,"$dir/$name$suffix\_".MindCommons::get_random.".gz") || LOGDIE "Can't rename file $filename: $!\n";
	  return EXIT_NO_FILE;
	}
    } elsif ($mime_type eq 'application/x-tar; charset=binary') {
	if ($suffix ne ".tar") {
	  DEBUG "Rename $filename to tar.\n";
	  move($filename,"$dir/$name$suffix\_".MindCommons::get_random.".tar") || LOGDIE "Can't rename file $filename: $!\n";
	  return EXIT_NO_FILE;
	}
    } elsif ($mime_type eq 'inode/x-empty; charset=binary') {
	DEBUG "Empty file $filename.\n";
	unlink $filename || LOGDIE "Can't delete file $filename: $!\n";
	return EXIT_EMPTY;
    } else {
	WARN "Unknown mime type: $mime_type for file $filename\n";
	return EXIT_WRONG_MIME;
    }
    ## we have only archives now
    my $ae = Archive::Extract->new( archive => $filename );
    my $tmp_dir = "$config->{dir_paths}->{filetmp_dir}/".MindCommons::get_random;
    make_path($tmp_dir);
    DEBUG "Extracting file $filename to $tmp_dir\n";
    eval {$ae->extract( to => $tmp_dir ) or LOGDIE  $ae->error;};
    if ($@) {;
	ERROR "Error in extract for $filename (mime: $mime_type): $@\n";
	return EXIT_EXTR_ERR;
    }
    TRACE "Extracted files from $filename to $tmp_dir:\n".Dumper($ae->files);
    ## move extracted files back
    foreach (MindCommons::find_files_recursively($tmp_dir)){
	next if ! -f $_;
	my ($name_e,$dir_e,$suffix_e) = fileparse($_, qr/\.[^.]*/);
	my $new_name = "$dir/$name_e"."_".MindCommons::get_random."$suffix_e";
	TRACE "Moving $_ to $new_name.\n";
	move("$_", $new_name) || LOGDIE "Can't rename file $_: $!\n";
    }
    remove_tree($tmp_dir) || LOGDIE "Can't delete dir $tmp_dir: $!\n";
    unlink $filename || LOGDIE "Can't delete file $filename: $!\n";
    return EXIT_NO_FILE;
}

sub run {
    my ($data, $dbh, $flm) = @_;
    $data->{customer_name} = $dbh->get_customer_name($data->{customer_id});
    $data->{host_name} = $dbh->get_host_name($data->{host_id});

    my $filename = $data->{file_name};
    my $ret = extract_file($filename, $flm);
    if ($ret == START_PARSERS) {
	my ($name, $dir, $suffix) = fileparse($data->{file_name}, qr/\.[^.]*/);
	if ($name =~ m/^(([a-z][a-z0-9_]*?)(statistics?|info))/i) {
	    my $table_name = lc($1)."_$data->{host_id}";
	    my $app = $2;
	    my $type = lc($3);
	    my $plugin_name = lc($app);
	    $type = "statistics" if $type eq "statistic"; ## fix for asc
	    my $columns = ['customer_id', 'host_id', 'inserted_in_tablename', 'worker_type', 'app_name', 'plugin_name'];
	    my $values = [$data->{customer_id}, $data->{host_id}, $dbh->getQuotedString($table_name), $dbh->getQuotedString($type), $dbh->getQuotedString($app), $dbh->getQuotedString($plugin_name)];
	    my $plugin_id = $dbh->getIDUsed ($config->{db_config}->{plugins_table}, $columns, $values);
	    if (! defined $plugin_id) {
		## add new plugin row
		$dbh->insertRowsTable ($config->{db_config}->{plugins_table}, $columns, $values);
		## retrieve the pluginid
		$plugin_id = $dbh->getIDUsed ($config->{db_config}->{plugins_table}, $columns, $values);
	    }
	    LOGDIE "Can't detect plugin id." if ! defined $plugin_id || $plugin_id !~ m/^[0-9]+$/;
	    $dbh->setNeedsUpdate ($plugin_id);
	    ## set the new pluginid to the file
	    $dbh->updateFileColumns ($data->{id}, ['plugin_id'], [$plugin_id]);
	    $dbh->increasePluginQueue($plugin_id);
	} else {
	    $ret = EXIT_STATUS_NA;  ## we don't know what to do with this, so we ignore it
	}
    }
    $dbh->updateFileColumns($data->{id}, ['status'], [$ret]);
    ## we always return success
    return 0;
}

return 1;
