package ExtractFiles;

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
    my $filename = shift;
# use Time::HiRes qw( usleep tv_interval gettimeofday);
# my $t0 = [gettimeofday];

    return EXIT_NO_FILE if ! -f $filename;

    my $flm = File::LibMagic->new();
    my ($name,$dir,$suffix) = fileparse($filename, qr/\.[^.]*/);
    my $mime_type = $flm->checktype_filename($filename);
# WARN Dumper(tv_interval($t0));
    my $filetmp_dir = $config->{dir_paths}->{filetmp_dir};
    make_path($filetmp_dir);

    DEBUG "Trying to extract $filename\n";
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
    my $tmp_dir = "$filetmp_dir/".MindCommons::get_random;
    make_path($tmp_dir);
    DEBUG "Extracting file $filename to $tmp_dir\n";
    eval {$ae->extract( to => $tmp_dir ) or LOGDIE  $ae->error;};
    if ($@) {;
	ERROR "Error in extract for $filename (mime: $mime_type): $@\n";
	return EXIT_EXTR_ERR;
    }
    DEBUG "Extracted files from $filename to $tmp_dir:\n".Dumper($ae->files);
    foreach (MindCommons::find_files_recursively($tmp_dir)){
	next if ! -f $_;
	my ($name_e,$dir_e,$suffix_e) = fileparse($_, qr/\.[^.]*/);
	my $new_name = "$dir/$name_e"."_".MindCommons::get_random."$suffix_e";
	DEBUG "Moving $_ to $new_name.\n";
	move("$_", $new_name) || LOGDIE "Can't rename file $_: $!\n";
    }
    remove_tree($tmp_dir) || LOGDIE "Can't delete dir $tmp_dir: $!\n";
    unlink $filename || LOGDIE "Can't delete file $filename: $!\n";
    return EXIT_NO_FILE;
}

sub start {
    my ($hash, $dbh) = @_;
    foreach my $id (keys %$hash){
	my $ret = extract_file($hash->{$id}->{file_name});
	if ($ret == START_PARSERS) {
# 	    my $filename = extract_file($hash->{$id}->{file_name};
	    my ($name, $dir, $suffix) = fileparse($hash->{$id}->{file_name}, qr/\.[^.]*/);
	    if ($name =~ m/^((.*?)(statistics?|info))/i) {
		my $table_name = lc($1)."_$hash->{$id}->{host_id}";
		my $app = $2;
		my $type = lc($3);
		LOGDIE "Wrong table name: $table_name" if $table_name !~ m/^[a-z0-9_]+$/i;
		my $plugin_name = lc($app);
		## fix asc name
		$type = "statistics" if $type eq "statistic";
		my $columns = ['customer_id', 'host_id', 'inserted_in_tablename', 'worker_type', 'app_name', 'plugin_name'];
		my $values = [$hash->{$id}->{customer_id}, $hash->{$id}->{host_id}, $dbh->getQuotedString($table_name), $dbh->getQuotedString($type), $dbh->getQuotedString($app), $dbh->getQuotedString($plugin_name)];
		$dbh->insertRowsDB ($config->{db_config}->{mind_plugins}, $columns, $values);
		my $plugin_id = $dbh->getIDUsed ($config->{db_config}->{mind_plugins}, $columns, $values);
		$dbh->updateFileColumns ($id, ['plugin_id'], [$plugin_id]);
	    } else {
		$ret = EXIT_STATUS_NA;  ## we don't know what to do with this
	    }
	}

	$hash->{$id}->{return_result} = $ret;
    }
    return START_PARSERS;
}

sub finish {
  my ($ret, $data, $dbh) = @_;
    foreach my $id (keys %$data){
	my $status = $data->{$id}->{return_result};
	$status = $data->{$id}->{status} if $data->{$id}->{status} > ERRORS_START;
	$status = EXIT_FILE_BAD if $data->{$id}->{customer_id} eq EXIT_STATUS_NA."" || 
				  $data->{$id}->{host_id} eq EXIT_STATUS_NA."";
	$dbh->updateFileColumns ($id, ['status'], [$status]);
	MindCommons::moveFiles($status, $data->{$id}, $dbh) if $status != $data->{$id}->{return_result}; ## aka error
    }
}

return 1;
