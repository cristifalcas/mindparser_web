#!/usr/bin/perl -w
# perl, munin (make perl-Module-Build)
# perl-Linux-Inotify2 perl-Log-Log4perl perl-XML-Simple perl-Digest-SHA perl-File-Copy-Recursive perl-Archive-Extract perl-Archive-Zip perl-File-LibMagic perl-DBD-MySQ perl-IO-Socket-INET6
my @crt_timeData = localtime(time);
foreach (@crt_timeData) {$_ = "0$_" if($_<10);}
print "Start: ". ($crt_timeData[5]+1900) ."-".($crt_timeData[4]+1)."-$crt_timeData[3] $crt_timeData[2]:$crt_timeData[1]:$crt_timeData[0].\n";

use warnings;
use strict;
$| = 1;

use File::Copy;
use File::Path qw(make_path remove_tree);
use File::Basename;
use Cwd 'abs_path';
use Linux::Inotify2;
use Time::Local;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Time::HiRes qw( usleep tv_interval gettimeofday);
use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init({ level   => $WARN,
#                            file    => ">>test.log" 
			   layout   => "%d [%5p] (%6P) %m%n",
});
use POSIX ":sys_wait_h";

use lib (fileparse(abs_path($0), qr/\.[^.]*/))[1]."our_perl_lib/lib"; 
use Mind_work::SqlWork;

my $script_path = (fileparse(abs_path($0), qr/\.[^.]*/))[1]."";
my $config = MindCommons::xmlfile_to_hash("config.xml");
my $uploads_dir = $config->{dir_paths}->{uploads_dir};
make_path($uploads_dir, $config->{dir_paths}->{filedone_dir}, $config->{dir_paths}->{fileerr_dir});

my $threads_stats = 50;
my $threads_extract = 10;
my $threads_munin = 10;
my $threads_log = 0;
my $dbh;

use Definitions ':all';

my $inotify = Linux::Inotify2->new;

my $watched_folders;
my $thread_name = "main";

sub reap_children {
    my $running = shift;
    my $thread_nr;
    my $pid = waitpid(-1, WNOHANG);
    my $exit_status = $? >> 8;
    if ($pid > 0) {
	LOGDIE  "Unknown pid: $pid.\n".Dumper($running) if ! defined $running->{$pid};
	$thread_nr = $running->{$pid}->{'thread_nr'};
	DEBUG "child $pid died, from id ".$running->{$pid}->{'fileid'}." with status=$exit_status: reapead.\n";
	## less then ERRORS_START should be succes. we keep failed because we don't want to work on them again
	delete $running->{'0'}->{$running->{$pid}->{'fileid'}} if $exit_status < ERRORS_START && $exit_status > EXIT_STATUS_NA;
	delete $running->{$pid};
	LOGDIE  "Thread number should be positive.\n" if $thread_nr < 1;
	return $thread_nr;
    }
    return undef;
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
	my $data = $getwork->();
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
		$finishwork->($ret, $id, $data->{$id});
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
	INFO "\n"."*" x 50 ."\n$string"."*" x 50 ."\n";
	sleep 3;
    };
}

sub munin_getwork {
    my $ret = $dbh->getWorkForMunin(START_MUNIN);
    return $ret;
}

sub munin_work {
    my $host_id = shift;
    use Mind_work::MuninWork;
    $0 = "munin_work_$host_id";
    my $ret = MuninWork::run($host_id);
    return $ret;
}

sub munin_finish {
    my ($id, $ret) = @_;
    if ($ret == SUCCESS_LAST) {
	$ret = MuninWork::finishedWork($id, $ret, START_MUNIN);
	return SUCCESS_LAST;
    } else {
	INFO "Munin did not finish succesfully: $ret\n";
	return EXIT_MUNIN_ERROR;
    }
}

sub statistics_getwork {
    my $ret = $dbh->getFilesForParsers(START_STATS);
    return $ret;
}

sub statistics_finish {
    my ($ret, $id, $data) = @_;
    moveFiles($ret, $id, $data);
}

sub statistics_worker {
    my ($id, $data) = @_;
    $0 = "parse_stats_$0";
    use Mind_work::ParseStats;
    my $ret = ParseStats::insertStats($id, $data, $dbh);
    return $ret;
}

sub extract_worker {
    my ($id, $data) = @_;
# my $t0 = [gettimeofday];
# print Dumper($id,$data);
    use Mind_work::ExtractFiles;
    my $ret = ExtractFiles::try_to_extract($data);
    foreach my $id (keys %$ret){
	my $status = $ret->{$id}->{return_result};
	$status = $ret->{$id}->{status} if $ret->{$id}->{status} > ERRORS_START;
	$status = EXIT_FILE_BAD if $ret->{$id}->{app_name} eq EXIT_STATUS_NA."" || 
				  $ret->{$id}->{customer_id} eq EXIT_STATUS_NA."" || 
				  $ret->{$id}->{host_id} eq EXIT_STATUS_NA."" || 
				  $ret->{$id}->{inserted_in_tablename} eq EXIT_STATUS_NA."" || 
				  $ret->{$id}->{worker_type} eq EXIT_STATUS_NA."";
	$dbh->updateFileStatus ($id, $status);
	moveFiles($status, $id, $ret->{$id}) if $status != $ret->{$id}->{return_result}; ## aka error
    }
# print Dumper(tv_interval($t0));
    return IGNORE;
}

sub extract_getwork {
    my $ret = $dbh->getWorkForExtract(START_EXTRACT);
    return $ret;
}

sub extract_finish {
    my ($ret, $id, $data) = @_;
}

sub moveFiles {
    my ($ret, $id, $data) = @_;
    my $filename = $data->{file_name};
    LOGDIE "worng data:".Dumper($ret, $id, $data) if ! (defined $data->{customer_id} && $data->{customer_id} > 0);
    my $cust_name = $dbh->get_customer_name($data->{customer_id});
    my $host_name = $dbh->get_host_name($data->{host_id});
    my $dir_prefix;
    if ($ret > ERRORS_START) { #error
	WARN "Returned error $ret for $id = $filename.\n";
	$dir_prefix = "$config->{dir_paths}->{fileerr_dir}/$cust_name/$host_name/errcode_$ret/";
    } elsif ($ret > EXIT_STATUS_NA && $ret <= ERRORS_START) { #normal: 
	DEBUG "Returned success $ret for $id = $filename.\n";
	$host_name = "deleted" if ! defined $host_name;
	$dir_prefix = "$config->{dir_paths}->{filedone_dir}/$cust_name/$host_name/";
    } else {
	LOGDIE "what is this?: $ret\n";
    }
    make_path($dir_prefix);
    my ($name,$dir,$suffix) = fileparse($filename, qr/\.[^.]*/);
    my $new_name = "$dir_prefix/$name"."_".MindCommons::get_random."$suffix";
    DEBUG "Moving $filename to $new_name\n" if -f $filename;
    move("$filename", $new_name);
    $dbh->updateFileStatus($id, $ret);
}

sub extractInfo {
    my $name = shift;
    my ($table_name, $type, $app);

    if ($name =~ m/^((.*)?(statistics?|info))/i) {
	$table_name = lc($1);
	$app = $2;
	$type = lc($3);
	## fix asc name
	$type = "statistics" if $type eq "statistic";
    }
    my $hash->{table_name} = $table_name;
    $hash->{app} = $app;
    $hash->{type} = $type;
    return $hash;
}

sub insertFile {
    my $file = shift;
    return if -d $file;
    DEBUG "Try to insert file $file.\n";
    my $status;
    my $file_hash;
    if ( -f $file ) {
	my ($name, $dir, $suffix) = fileparse($file, qr/\.[^.]*/);
	$file_hash->{file_info}->{size} = -s $file;
	$file_hash->{file_info}->{name} = $file;
	$file_hash->{file_info}->{md5} = MindCommons::get_file_sha($file);
	($file_hash->{machine}->{customer}, $file_hash->{machine}->{host}) = $dir =~ m/^$uploads_dir\/*([^\/]+)\/+([^\/]+)\/+$/;
	$file_hash->{worker} = extractInfo($name);
	$status = START_EXTRACT;
    } else {
# 	$status = EXIT_NO_FILE;
	return;
    }
    my $id = $dbh->insertFile ($status, $file_hash);
}

sub cleanAndExit {
    WARN "Killing all child processes\n";
    kill 9, map {s/\s//g; $_} split /\n/, `ps -o pid --no-headers --ppid $$`;
    exit 1000;
}

sub main_process_worker {
    INFO "Starting main thread: watching for files and adding them to the db.\n";
    my $main_pid = $$;
    use sigtrap 'handler' => \&cleanAndExit, 'INT', 'ABRT', 'QUIT', 'TERM';


    ## since we never exit, we will probably never take care of those threads
    my ($forks, $pid);
    $forks->{$main_pid} = "main";
    $pid = fork();
    if (!$pid) {focker_launcher(\&extract_worker, \&extract_getwork, \&extract_finish, $threads_extract); exit 0;};
    $forks->{$pid} = "extract";
    $pid = fork();
    if (!$pid) {focker_launcher(\&statistics_worker, \&statistics_getwork, \&statistics_finish, $threads_stats); exit 0;};
    $forks->{$pid} = "statistics";
#     $pid = fork();
#     if (!$pid) {focker_launcher(\&logparser_worker, \&logparser_getwork, \&logparser_finish, $threads_log); exit 0;};
#     $forks->{$pid} = "logs";
    $pid = fork();
    if (!$pid) {sleep 5;focker_launcher(\&munin_worker, \&munin_getwork, \&munin_finish, $threads_munin); exit 0;};
    $forks->{$pid} = "munin";
    $pid = fork();
    if (!$pid) {periodic_checks($forks, $main_pid); exit 0;};
    $forks->{$pid} = "checks";
    DEBUG Dumper($forks);

    $dbh = new SqlWork();
    assign_watchers($uploads_dir);
    insertFile ($_) foreach (MindCommons::find_files_recursively($uploads_dir));

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

sub watch_handler {
    my $e = shift;
    assign_watchers($e->fullname) if (-d $e->fullname);
}

sub assign_watchers {
    my $path = shift;
    my @files = MindCommons::find_files_recursively($path);
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
exit ERRORS_LAST;

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

