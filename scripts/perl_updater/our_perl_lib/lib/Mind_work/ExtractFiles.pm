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
	  move($filename, "$dir/$name$suffix\_".MindCommons::get_random.".log");
	  return EXIT_NO_FILE; ## file is gone now
      }
      DEBUG "Extract has nothing to do with file $name$suffix.\n";
      return START_STATS;
    } elsif ($mime_type eq 'application/zip; charset=binary') {
	if ($suffix ne ".zip") {
	  DEBUG "Rename $filename to zip.\n";
	  move($filename,"$dir/$name$suffix\_".MindCommons::get_random.".zip");
	  return EXIT_NO_FILE;
	}
    } elsif ($mime_type eq 'application/x-gzip; charset=binary') {
	if ($suffix ne ".gz" && $suffix ne ".tgz") {
	  DEBUG "Rename $filename to gz.\n";
	  move($filename,"$dir/$name$suffix\_".MindCommons::get_random.".gz");
	  return EXIT_NO_FILE;
	}
    } elsif ($mime_type eq 'application/x-tar; charset=binary') {
	if ($suffix ne ".tar") {
	  DEBUG "Rename $filename to tar.\n";
	  move($filename,"$dir/$name$suffix\_".MindCommons::get_random.".tar");
	  return EXIT_NO_FILE;
	}
    } elsif ($mime_type eq 'inode/x-empty; charset=binary') {
	DEBUG "Empty file $filename.\n";
	unlink $filename;
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
	move("$_", $new_name);
    }
    remove_tree($tmp_dir);
    unlink $filename;
    return EXIT_NO_FILE;
}

sub start {
    my $hash = shift;
    foreach my $id (keys %$hash){
	my $ret = extract_file($hash->{$id}->{file_name});
	$hash->{$id}->{return_result} = $ret;
    }
    return $hash;
}

sub finish {
  my ($ret, $id, $data) = @_;
}

return 1;
