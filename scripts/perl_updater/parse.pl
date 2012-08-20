#!/usr/bin/perl -w
my @crt_timeData = localtime(time);
foreach (@crt_timeData) {$_ = "0$_" if($_<10);}
print "Start: ". ($crt_timeData[5]+1900) ."-".($crt_timeData[4]+1)."-$crt_timeData[3] $crt_timeData[2]:$crt_timeData[1]:$crt_timeData[0].\n";

use warnings;
use strict;
$| = 1;
$SIG{__WARN__} = sub { die @_ };

use File::Find;
use File::Copy;
use File::Path qw(make_path remove_tree);
use File::Basename;
use File::Slurp;
use Cwd 'abs_path';
use Linux::Inotify2;
use Time::Local;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Time::HiRes qw( usleep tv_interval gettimeofday);
use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({ level   => $DEBUG,
#                            file    => ">>test.log" 
			   layout   => "%d [%5p] (%6P) %m%n",
});
use POSIX ":sys_wait_h";

use lib (fileparse(abs_path($0), qr/\.[^.]*/))[1]."our_perl_lib/lib"; 
use Mind_work::SqlWork;

my $script_path = (fileparse(abs_path($0), qr/\.[^.]*/))[1]."";
my $config = MindCommons::xmlfile_to_hash("config.xml");
my $uploads_dir = $config->{dir_paths}->{uploads_dir};
my $filedone_dir = $config->{dir_paths}->{filedone_dir};
my $fileerr_dir = $config->{dir_paths}->{fileerr_dir};
MindCommons::makedir($uploads_dir);
MindCommons::makedir($filedone_dir);
MindCommons::makedir($fileerr_dir);

my $threads_stats = 1;
my $threads_munin = 1;
my $threads_log = 0;
my $dbh;

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

my $inotify = Linux::Inotify2->new;

my $watched_folders;
my $thread_name = "main";

sub reap_children {
    my $running = shift;
    my $thread_nr;
    my $pid = waitpid(-1, WNOHANG);
    if ($pid > 0) {
	my $exit_status = $?;
	LOGDIE  "Unknown pid: $pid.\n".Dumper($running) if ! defined $running->{$pid};
	$thread_nr = $running->{$pid}->{'thread_nr'};
	DEBUG "child $pid died, from id ".$running->{$pid}->{'fileid'}." with status=$exit_status: reapead.\n";
	delete $running->{'0'}->{$running->{$pid}->{'fileid'}} if $exit_status > 0 && $exit_status < 100;
	delete $running->{$pid};
	LOGDIE  "Thread number should be positive.\n" if $thread_nr < 1;
	return $thread_nr;
    }
    return undef;
}

sub parser_getwork {
    my $ret = $dbh->getFilesForParsers();
    return $ret;
}

sub parser_finish {
    my ($id, $ret) = @_;
#     $thread_name = $thread_name."_".$id;
    my $hash = $dbh->getFile($id);
    my $filename = $hash->{file_name};
    my $cust_name = $dbh->get_customer_name($hash->{customer_id});
    my $host_name = $dbh->get_host_name($hash->{host_id});

    my $dir_prefix;
    if ($ret > 100) { #error
	WARN "Returned error $ret for $id = $filename.\n";
	$dir_prefix = "$fileerr_dir/$cust_name/$host_name/errcode_$ret/";
    } elsif ($ret == 0) { #file was ignored by the function
	return $ret; 
    } else { #normal: >0 <=100
	$dir_prefix = "$filedone_dir/$cust_name/$host_name/";
    }
    MindCommons::makedir($dir_prefix);
    my ($name,$dir,$suffix) = fileparse($filename, qr/\.[^.]*/);
    my $new_name = "$dir_prefix/$name"."_".MindCommons::get_random."$suffix";
    DEBUG "Moving $filename to $new_name\n";
    move("$filename", $new_name);
    $dbh->updateFileStatus($id, $ret);
    return $ret;
}

sub munin_getwork {
    my $ret = $dbh->getWorkForMunin(EXIT_STATS_SUCCESS);
    return $ret;
}

sub munin_update {
    my ($host_id, $files) = @_;
    use Mind_work::MuninWork;
    $0 = "munin_update_$0";
    INFO "running munin for host=$host_id\n";
    my $ret = MuninWork::run($dbh, $host_id, $files);
    return $ret;
}

sub munin_finish {
    my ($id, $status) = @_;
    if ($status == EXIT_MUNIN_SUCCESS) {
	$dbh->doneWorkForMunin($id, $status, EXIT_STATS_SUCCESS);
    } else {
	INFO "Munin did not finish succesfully: $status\n";
    }
}

sub focker_launcher {
    my ($dowork, $getwork, $finishwork, $max_procs) = @_;
    my $running;
    $dbh = new SqlWork();
    my @thread = (1..$max_procs);
    use B qw(svref_2object);
    my $cv = svref_2object ( $dowork );
    $thread_name = $cv->GV->NAME;
    $0 = "main_".$thread_name;
    INFO "Starting forker process $thread_name.\n";

    while (1) {
	my $data = $getwork->($dbh);
	foreach (keys %$data) {
	    delete $data->{$_} if defined $running->{0}->{$_};
	}
	my $id = (keys %$data)[0]; ## first element 
	if ((scalar keys %$running) <= $max_procs && defined $id && (! defined $running->{0}->{$id})){
	    $running->{0}->{$id} = 1;
	    my $crt = shift @thread;  # get a number
	    LOGDIE  "We should always have something.\n" if ! defined $crt;
	    my $pid = fork();
	    if (! defined ($pid)){
		LOGDIE  "Can't fork $thread_name with id=$id.\n";
	    } elsif ($pid==0) {
		$0 = "$crt";
		DEBUG "Starring $thread_name with id=$id\n";
		$dbh->cloneForFork();
		my $ret = $dowork->($id, $data->{$id});
		$finishwork->($id, $ret);
		DEBUG "Done $thread_name with status $ret (id=$id).\n";
		$dbh->disconnect;
		exit $ret;
	    }
	    LOGDIE  "Seems me want to add the same process twice.\n" if defined $running->{$pid};
	    $running->{$pid}->{'thread_nr'} = $crt;
# 	    $running->{$pid}->{'filename'} = $filename;
	    $running->{$pid}->{'fileid'} = $id;
	}

	my $thread_nr;
	do {
	    $thread_nr = reap_children($running);
	    push @thread, $thread_nr if defined $thread_nr;
	} while (defined $thread_nr);
	usleep(100000);
# 	sleep 1;
    }

    do {
	my $thread_nr = reap_children($running);
	push @thread, $thread_nr;
	usleep(100000);
    } while (scalar keys %$running);

    INFO "FIN *******************.\n";
}

sub periodic_checks {
    my ($forks, $main_pid) = @_;
    INFO "Starting checks process.\n";
    $forks->{$$} = 'checks';
    our ($string, $nr);
    sub get_kids {
	my ($forks, $pid_p) = @_;
	## parent stats
	open( STAT , "</proc/$pid_p/stat" ) or return;
	my @stat = split /\s+/ , <STAT>;
	close( STAT );
	my $crt_nr = $nr;
	$forks->{$pid_p} = $stat[1] if ! defined $forks->{$pid_p};
	$string .= "**". " "x($nr*2) . " "x($nr?1:0) ."worker pid = $pid_p, VmSize = ".(sprintf "%.0f", $stat[22]/1024/1024)."MB, VmRSS =".(sprintf "%.0f", $stat[23] * 4/1024)."MB, daddy = $stat[3] name = $forks->{$pid_p}\n";
	$nr++;
	
	get_kids($forks, $_) foreach (map {s/\s//g; $_} split /\n/, `ps -o pid --no-headers --ppid $pid_p`);
	  $nr =$crt_nr;
    }
    while (1) {
	($nr, $string) = (0,"");
	get_kids($forks, $main_pid);
	DEBUG "\n"."*" x 50 ."\n$string"."*" x 50 ."\n";
	sleep 30;
    };
}

sub get_table_name {
    my $name = shift;
    my ($table_name, $type, $app);

    if ($name =~ m/^((.*)?(statistics|info))/i) {
	$table_name = lc($1);
	$app = $2;
	$type = lc($3);
    }
    return ($table_name, $app, $type);
}

sub try_to_extract {
    use Archive::Extract;
    use File::LibMagic;

    my $filename = shift;
    my $flm = File::LibMagic->new();
    my ($name,$dir,$suffix) = fileparse($filename, qr/\.[^.]*/);
    my $mime_type = $flm->checktype_filename($filename);

    my $filetmp_dir = $config->{dir_paths}->{filetmp_dir};
    MindCommons::makedir($filetmp_dir);

    DEBUG "Trying to extract $name$suffix\n";
    if ($mime_type eq 'text/plain; charset=us-ascii') {
	if ( $suffix ne ".log"){
	  DEBUG "Rename $filename to log.\n";
	  move($filename, "$dir/$name.log");
	  return EXIT_NO_FILE; ## file is gone now
      }
      return EXIT_IGNORE;
    } elsif ($mime_type eq 'application/zip; charset=binary') {
	if ($suffix ne ".zip") {
	  DEBUG "Rename $filename to zip.\n";
	  move($filename,"$dir/$name$suffix.zip");
	  return EXIT_NO_FILE;
	}
    } elsif ($mime_type eq 'application/x-gzip; charset=binary') {
	if ($suffix ne ".gz" && $suffix ne ".tgz") {
	  DEBUG "Rename $filename to gz.\n";
	  move($filename,"$dir/$name$suffix.gz");
	  return EXIT_NO_FILE;
	}
    } elsif ($mime_type eq 'application/x-tar; charset=binary') {
	if ($suffix ne ".tar") {
	  DEBUG "Rename $filename to tar.\n";
	  move($filename,"$dir/$name$suffix.tar");
	  return EXIT_NO_FILE;
	}
    } else {
	WARN "Unknown mime type: $mime_type for file $filename\n";
	return EXIT_WRONG_MINE;
    }
    my $ae = Archive::Extract->new( archive => $filename );
    my $tmp_dir = "$filetmp_dir/".MindCommons::get_random;
    MindCommons::makedir($tmp_dir);
    DEBUG "Extracting in $thread_name file $filename to $tmp_dir\n";
    eval {$ae->extract( to => $tmp_dir ) or LOGDIE  $ae->error;};
    if ($@) {;
	ERROR "$thread_name Error in extract for $filename (mime: $mime_type): $@\n";
	return EXIT_EXTR_ERR;
    }
    DEBUG "Extracted in $thread_name files from $filename to $tmp_dir:\n".Dumper($ae->files);
    foreach (find_files_recursively($tmp_dir)){
	next if ! -f $_;
	my ($name_e,$dir_e,$suffix_e) = fileparse($_, qr/\.[^.]*/);
	my $new_name = "$dir/$name_e"."_".MindCommons::get_random."$suffix_e";
	INFO "$thread_name Moving $_ to $new_name.\n";
	move("$_", $new_name);
    }
    remove_tree($tmp_dir);
    unlink $filename;
    return EXIT_IGNORE;
}

sub parse_statistics {
    my $fileid = shift;
    $0 = "parse_stats_$0";
    
    my $hash = $dbh->getFile($fileid);
    my $cust_name = $dbh->get_customer_name($hash->{customer_id});
    my $host_name = $dbh->get_host_name($hash->{host_id});
    my $filename = $hash->{file_name};
    return EXIT_NO_FILE if (! -f $filename);
    my ($name, $dir, $suffix) = fileparse($filename, qr/\.[^.]*/);
    my ($table_name, $app, $type) = get_table_name("$name$suffix");
    $table_name .= "_".lc($cust_name);
    $table_name = substr($table_name, 0, 64);  ## 64 is the max size of table name
    if (! defined $type || $type ne "statistics") {
	DEBUG "Probably not for $0: $name$suffix from $cust_name, machine $host_name: ".Dumper("$name$suffix",$table_name, $app, $type);
	return EXIT_IGNORE;
    }
    INFO "Parsing $0 file $name$suffix (id=$fileid) as $table_name from $cust_name, machine $host_name.\n";
    open (MYFILE, "$filename") or LOGDIE  "Couldn't open $filename: $!";
    $dbh->createStatsTable($table_name);
    my (@header, $header_hash, $columns, @values);
    my $t0 = [gettimeofday];my $t1=0;my $t2=0;
    my ($count, $second, $extra_arr) = (0, 1, 0);
    while (<MYFILE>) {
	my $line = $_;
	chomp $line;
	my @arr = map {s/(\r)|(^\s*)|(\s*$)//g; $_ } split /,/, $line;
	$count++;
	if ($count == 1) {
	    if ($line !~ m/^Date,\s*Time,/i){
	      close (MYFILE); 
	      return EXIT_WRONG_TYPE ;
	    }
	    @header = @arr;
	    $dbh->add_new_columns ($table_name, \@header);
	} else {
	    ## no support for new fields in middle of the line
	    if ( (scalar @arr) + $extra_arr != scalar @header || $arr[0] !~ m/[0-9]{2}\/[0-9]{2}\/[0-9]{2}/ ){
		DEBUG "filename = $filename\n\trow=$count\n\tarr=".(scalar @arr)."\n\textra=$extra_arr\n\theader_arr=".(scalar @header)."\n\tfirst=$arr[0]\n\tsecond=$arr[1])\n";
		LOGDIE "Strange line nr $count\n\t$line\n\tfrom file $filename\n" if $arr[0] ne "Date";
		next;
	    }

	    my ($d, $m, $y) = split '/', $arr[0]; #21/07/12
	    my ($h, $min, $s) = split ':', $arr[1]; #03:00:15
	    my $timestamp = timelocal($s,$min,$h,$d,$m-1,$y+2000); ## or timegm
	    unshift @arr, $timestamp;
	    if ($second) {  ## fix header 
	      unshift @header, 'timestamp';
	      $extra_arr += 1; ## 1 from timestamp inserted from date+time
	      my @header_cp = @header;
	      while (my ($index, $val) = each @arr) {
		if ($val =~ m/;/) {
		  my $i=0;
		  my @q = map {$header_cp[$index]."_".$i++} split /;/, $val;
		  $extra_arr += scalar @q - 1; ## 1 from scalar @q replaces 1 element
		  splice(@header, $index, 1, @q); 
		  $dbh->add_new_columns ($table_name, \@q);
		}
	      }
	      $header_hash = $dbh->get_md5_names(@header);
# print Dumper("as",$header_hash,@header);
	      $second = 0;
	    }
	    @arr = map{split /;/} @arr;
	    my $new_vals;
	    ## 1=timestamp, 2=date, 3=time
	    for (my $i = 3; $i < scalar @header; $i++) {
# print Dumper($header_hash->{$header[$i]}, $header_hash, @header, $i);
		$columns .= ",$header_hash->{$header[$i]}" if $count == 2;
		$new_vals .= ", $arr[$i]";
	    }
	    push @values, " ($fileid, ".$hash->{host_id}.", $arr[0], '$arr[1]', '$arr[2]' $new_vals) ";
	    my $e=tv_interval($t0);$t2 += $e;$t1 += $e;
	    if ($count % 200 == 0){
		$dbh->insertRowsDB($table_name, "(file_id, host_id, $header[0], date, time $columns)", join ",", @values);
		@values = ();
		DEBUG "Insert 200 rows for $0 took $t2\n";$t2=0;
	    };
	    $t0 = [gettimeofday];;
	}
    }
    close (MYFILE);
    $dbh->insertRowsDB($table_name, "(file_id, host_id, $header[0], date, time $columns)", join ",", @values) if scalar @values;
    $dbh->updateFileInfo($fileid, $t1, $table_name);
    return EXIT_NO_LINES if $count < 2;
    DEBUG "Total parse $0 for $name$suffix took $t1\n";
    INFO "Done stats $0 for file $name$suffix id $fileid\n";
    return EXIT_STATS_SUCCESS;
}

sub parse_logs {
    my $fileid = shift;
    $0 = "parse_logs_$0";
    my $hash = $dbh->getFile($fileid);
    my $cust_name = $dbh->get_customer_name($hash->{customer_id});
    my $host_name = $dbh->get_host_name($hash->{host_id});
    my $filename = $hash->{file_name};
    my ($name,$dir,$suffix) = fileparse($filename, qr/\.[^.]*/);
    my ($table_name, $app, $type) = get_table_name("$name$suffix");
    $table_name .= "_".lc($cust_name);
    $table_name = substr($table_name, 0, 64);  ## 64 is the max size of table name
    if (! defined $type || $type ne "info") {
	DEBUG "Probably not for $0: $name$suffix from $cust_name, machine $host_name.\n";
	return EXIT_IGNORE;
    }
    return EXIT_NO_FILE if (! -f $filename);

    open (MYFILE, "$filename") or LOGDIE  "Couldn't open $filename: $!";
    INFO "done logs\n";
    close (MYFILE); 
    return EXIT_IGNORE;
}

sub check_cust_host {
    my $file = shift;
    my ($name,$dir,$suffix) = fileparse($file, qr/\.[^.]*/);
    my $customers = $dbh->getCustomers;
    my ($file_c, $file_h) = $dir =~ m/^.+\/([^\/]+)\/+([^\/]+)\/$/;
    my ($cust_id, $host_id) = ($customers->{$file_c}->{'id'}, $customers->{$file_c}->{'hosts'}->{$file_h});
    LOGDIE  "Strange file $file\n".Dumper($customers) if ! defined $cust_id || ! defined $host_id;
    return ($cust_id, $host_id);
}

sub insertFile {
    my $file = shift;
#     my $ret = ;
# print Dumper($ret, EXIT_IGNORE);
    return EXIT_NO_FILE if ! -f $file || -d $file;
    return if ( try_to_extract($file) != EXIT_IGNORE);
    my ($name,$dir,$suffix) = fileparse($file, qr/\.[^.]*/);
    return EXIT_NO_FILE if "$name$suffix" =~ m/^\./;
    my ($cust_id, $host_id) = check_cust_host($file);
    INFO "Adding file $file.\n";
    $dbh->insertFile ($cust_id, $host_id, $file, -s $file,  EXIT_STATS_EXPECTS);
}

sub main_process_worker {
    INFO "Starting main thread: watching for files and adding them to the db.\n";
    my $main_pid = $$;

    ## since we never exit, we will probably never take care of those threads
    my ($forks, $pid);
    $forks->{$main_pid} = "main";
    $pid = fork();
    if (!$pid) {focker_launcher(\&parse_statistics, \&parser_getwork, \&parser_finish, $threads_stats); exit 0;};
    $forks->{$pid} = "statistics";
    $pid = fork();
    if (!$pid) {focker_launcher(\&parse_logs, \&parser_getwork, \&parser_finish, $threads_log); exit 0;};
    $forks->{$pid} = "logs";
    $pid = fork();
    if (!$pid) {focker_launcher(\&munin_update, \&munin_getwork, \&munin_finish, $threads_munin); exit 0;};
    $forks->{$pid} = "munin";
    $pid = fork();
    if (!$pid) {periodic_checks($forks, $main_pid); exit 0;};
    $forks->{$pid} = "checks";
    DEBUG Dumper($forks);

    $dbh = new SqlWork();
    assign_watchers($uploads_dir);
    insertFile ($_) foreach (find_files_recursively($uploads_dir));

    while (1) {
	my @events = $inotify->read;
	unless (@events > 0){
	    ERROR "read error: $!\n";
	    last ;
	}

	foreach my $event (@events) {
	    if ($event->IN_DELETE_SELF) {
		DEBUG "Del dir ". $event->fullname . "\n";
		delete $watched_folders->{$event->fullname};
	    } elsif (($event->IN_CLOSE_WRITE || $event->IN_MOVED_FROM || $event->IN_MOVED_TO) && -f $event->fullname) {
# 		try_to_extract($event->fullname); ## ret code > 0 means problems
		insertFile ($event->fullname);
	    } elsif ($event->IN_CREATE && -d $event->fullname) {
		DEBUG "Add dir ".$event->fullname."\n";
	    }
	}
    }
    $dbh->disconnect;

    ## take care of threads (we should never reach this point)
    while (scalar keys %$forks) {
	$pid = waitpid(-1, WNOHANG);
	if ($pid > 0) {
	    next, WARN "strange pid: $pid\n" if ! defined  $forks->{$pid};
	    delete $forks->{$pid};
	    DEBUG "Reaped $pid\n";
	}
	usleep(100000);
    }
    INFO "FIN main *******************.\n";
}

sub find_files_recursively {
    my $path = shift;
    my @files;
    find(sub{push @files, $File::Find::name},$path);;
    return @files;
}

sub watch_handler {
    my $e = shift;
    assign_watchers($e->fullname) if (-d $e->fullname);
}

sub assign_watchers {
    my $path = shift;
    my @files = find_files_recursively($path);
    foreach my $file (@files){
	if (-d $file && ! defined $watched_folders->{$file}) {
	    INFO "Watching new dir $file.\n";
	    $inotify->watch($file, IN_CREATE|IN_CLOSE_WRITE|IN_DELETE_SELF|IN_MOVED_FROM|IN_MOVED_TO, \&watch_handler);
	    $watched_folders->{$file} = 1;
	}
    }
}

eval {main_process_worker();};
ERROR "Error in main thread: $@\n" if $@;
kill 9, map {s/\s//g; $_} split /\n/, `ps -o pid --no-headers --ppid $$`;
exit 1;

my $path_files = abs_path("d:\\temp\\rtslogs\\rts_logs\\");
my $header = {};
my $body = {};
my $others;
my @ignored;

sub add_document{
	my $file = shift;
	my @text = read_file( $file ) ;
#	return if $file ne "d:/temp/rtslogs/rts_logs/q";
	print "$file\n";
	parse_file(\@text);
}

print "-Start searching for files in $path_files dir.\n";
find sub { add_document ($File::Find::name) if -f && (/\.*$/i) }, $path_files if  (-d $path_files);
#print "+Done searching for files in $path_files dir:".(length($text)).".\n";

sub parse_file_mind {
	my $text = shift;
	my $count = 0;
	my $block;
	foreach my $line (@$text){
#		print "$count\r" if ! (++$count % 500);
		if ($line =~ m/^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d,\d{2,3})(.*)$/){
			if (defined $block && $block !~ m/^\s*$/) {
				parse_block($block);
			}
			$block = $line;
		} else {
			$block .= $line;
		}
	}
	parse_block($block);
}

sub parse_file_in {
	my $text = shift;
	my $count = 0;
	my $block;
	foreach my $line (@$text){
#		print "$count\r" if ! (++$count % 500);
		if ($line =~ m/^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d,\d{2,3})\s+([a-z]+):\s+ ([^\s]) \[DialogicMainThread\] [-]+ EVENT START [-]+$/i){
			if (defined $block && $block !~ m/^\s*$/) {
				parse_block_in($block);
			}
			$block = $line;
		} else {
			$block .= $line;
		}
	}
	parse_block_in($block);
}

