#!/usr/bin/perl -w
# perl, munin (make perl-Module-Build)
# perl-Linux-Inotify2 perl-Log-Log4perl perl-XML-Simple perl-Digest-SHA perl-File-Copy-Recursive perl-Archive-Extract perl-Archive-Zip perl-File-LibMagic perl-DBD-MySQ perl-IO-Socket-INET6
my @crt_timeData = localtime(time);
foreach (@crt_timeData) {$_ = "0$_" if($_<10);}
print "Start: ". ($crt_timeData[5]+1900) ."-".($crt_timeData[4]+1)."-$crt_timeData[3] $crt_timeData[2]:$crt_timeData[1]:$crt_timeData[0].\n";

use warnings;
use strict;
$| = 1;

use File::Path qw(make_path remove_tree);
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Log::Log4perl qw(:easy);
Log::Log4perl::init( \ <<'EOT' );
        log4perl.logger             = INFO, A1
	log4perl.filter.ExcludeMe = sub { !/Reading additional config from/ }
        log4perl.appender.A1        = Log::Log4perl::Appender::Screen
        log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
        log4perl.appender.A1.layout.ConversionPattern =  %d [%5p] (%6P) [%rms] [%M] - %m{chomp}	%x\n
        log4perl.appender.A1.Filter  = ExcludeMe
EOT

use Cwd 'abs_path';
use File::Basename;
use lib (fileparse(abs_path($0), qr/\.[^.]*/))[1]."our_perl_lib/lib"; 
use File::Copy;
use Definitions ':all';

my $script_path = (fileparse(abs_path($0), qr/\.[^.]*/))[1]."";
my $config = MindCommons::xmlfile_to_hash("config.xml");
my $uploads_dir = $config->{dir_paths}->{uploads_dir};
make_path($uploads_dir, $config->{dir_paths}->{filedone_dir}, $config->{dir_paths}->{fileerr_dir});

my $inotify = Linux::Inotify2->new;
my $threads_stats = 0;
my $threads_extract = 0;
my $threads_munin = 1;
my $threads_log = 0;
my $watched_folders;

sub reap_children {
    my $running = shift;
    use POSIX ":sys_wait_h";
    my $thread_nr;
    my $pid = waitpid(-1, WNOHANG);
    my $exit_status = $? >> 8;
    if ($pid > 0) {
	LOGDIE  "Unknown pid: $pid.\n".Dumper($running) if ! defined $running->{$pid};
	$thread_nr = $running->{$pid}->{'thread_nr'};
	DEBUG "child $pid died, from id ".$running->{$pid}->{'fileid'}." with status=$exit_status: reapead.\n";
	## less then ERRORS_START should be succes. we keep failed because we don't want to work on them again
	delete $running->{'0'}->{$running->{$pid}->{'fileid'}} if ! $exit_status;
	delete $running->{$pid};
	LOGDIE  "Thread number should be positive.\n" if $thread_nr < 1;
	return $thread_nr;
    }
    my $hash = $running->{'0'};
    foreach (keys %$hash){delete $running->{'0'}->{$_} if ($running->{'0'}->{$_}  < time() - 3600)};
    return undef;
}

sub getFunctionName {
    my $func = shift;
    use B qw(svref_2object);
    my $cv = svref_2object ( $func );
    return $cv->GV->NAME;
}

## getwork ($dbh): returns a hash with one element (some unique id). If working for this id failes, it will be ignored in the future
## dowork ($dbh, $data): returns one of Definitions.pm exitcodes.
sub focker_launcher {
    my ($dowork, $getwork, $max_procs, $args) = @_;
    return if $max_procs < 1;
    use Time::HiRes qw( usleep tv_interval gettimeofday);
    my $running;
    $running->{0} = undef;
    my $dbh = new SqlWork();
    my @thread = (1..$max_procs);
    Log::Log4perl::NDC->remove();Log::Log4perl::NDC->push(getFunctionName($dowork));

    while (1) {
	TRACE "Do getwork.\n";
	my $data = $getwork->($dbh);
	foreach (sort keys %$data) {
	    if (defined $running->{0}->{$_}){
		TRACE "Remove existing id $_\n";
		delete $data->{$_}; 
	    };
	}
	my ($failed_count, $queue_count, $running_count) = ((scalar keys %{ $running->{0} }), scalar keys %$data, (scalar keys %$running)-1);
	$failed_count -= $running_count;

	DEBUG "Got $failed_count failed, $queue_count in queue and $running_count already running (max=$max_procs).\n";
	my $id = (sort keys %$data)[0]; ## first element from what remains in $data
	if ($running_count < $max_procs && defined $id){
	    $running->{0}->{$id} = time();
	    my $crt = shift @thread;  # get a number
	    LOGDIE  "we should always have something.\n" if ! defined $crt;
	    my $pid = fork();
	    if (! defined ($pid)){
		LOGDIE  "Can't fork with id=$id.\n";
	    } elsif ($pid==0) {
		Log::Log4perl::NDC->remove();Log::Log4perl::NDC->push(getFunctionName($dowork));
		DEBUG "Starring with id=$id\n";
		$dbh->cloneForFork();
		my $ret = $dowork->($dbh, $data->{$id}, $args);
		DEBUG "Finish with status $ret (id=$id).\n";
		$dbh->disconnect;
		exit $ret;
	    }
	    LOGDIE  "Seems me want to add the same process twice.\n" if defined $running->{$pid};
	    $running->{$pid}->{'thread_nr'} = $crt;
	    $running->{$pid}->{'fileid'} = $id;
	}

	my $thread_nr;
	do {
	    $thread_nr = reap_children($running);
	    push @thread, $thread_nr if defined $thread_nr;
	} while (defined $thread_nr);

	usleep(10000);
	sleep 1 if !(scalar @thread && $queue_count-$failed_count>0);
    }

    $dbh->disconnect;
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
	$string .= "**". " "x($nr*2) . " "x($nr?1:0) ."worker pid = $pid_p, VmSize = ".(sprintf "%.0f", $stat[22]/1024/1024)."MB, VmRSS = ".(sprintf "%.0f", $stat[23] * 4/1024)."MB, daddy = $stat[3] name = $forks->{$pid_p}\n";
	$nr++;
	
	get_kids($forks, $_) foreach (map {s/\s//g; $_} split /\n/, `ps -o pid --no-headers --ppid $pid_p`);
	  $nr =$crt_nr;
    }
    while (1) {
	($nr, $string) = (0,"");
	get_kids($forks, $main_pid);
	DEBUG "\n"."*" x 50 ."\n$string"."*" x 50 ."\n";
	sleep 3;
    };
}

sub extract_getwork {
    my $dbh = shift;
    return $dbh->getWorkForExtract(START_EXTRACT);
}

sub statistics_getwork {
    my $dbh = shift;
    return $dbh->getWorkForStatsParsers(START_PARSERS);
}

sub logparser_getwork {
    my $dbh = shift;
    return $dbh->getWorkForLogParsers(START_PARSERS);
}

sub munin_getwork {
    my $dbh = shift;
    return $dbh->getWorkForMunin(START_MUNIN);
}

sub munin_worker {
    my ($dbh, $data, $args) = @_;
    use Mind_work::MuninWork;
    my $ret = MuninWork::run($data, $dbh);
    return $ret;
}

sub statistics_worker {
    my ($dbh, $data, $args) = @_;
    use Mind_work::ParseStats;
    my $ret = ParseStats::run($data, $dbh);
    return $ret;
}

sub extract_worker {
    my ($dbh, $data, $args) = @_;
    use Mind_work::ExtractFiles;
    my $ret = ExtractFiles::run($data, $dbh, $args);
    return $ret;
}

sub logparser_worker {
    my ($dbh, $data, $args) = @_;
    use Mind_work::ParseLogs;
    my $ret = ParseLogs::run($data, $dbh);
    return $ret;
}

sub addFilesDB {
# move from dirs to path, rename files starting with dot delete empty dirs
    my ($file, $dbh) = @_;
    if ( -f $file ) {
	DEBUG "Try to insert file $file.\n";
	my ($name, $dir, $suffix) = fileparse($file, qr/\.[^.]*/);
	my $file_hash->{file_info}->{size} = -s $file;
	$file_hash->{file_info}->{name} = $file;
	$file_hash->{file_info}->{md5} = MindCommons::get_file_sha($file);
	($file_hash->{machine}->{customer}, $file_hash->{machine}->{host}) = $dir =~ m/^$uploads_dir\/*([^\/]+)\/+([^\/]+)\/+$/;
	$dbh->insertFile ($file_hash, START_EXTRACT);
    }
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
    if (!$pid) {my $flm = File::LibMagic->new();INFO "Starting forker process extract.\n";focker_launcher(\&extract_worker, \&extract_getwork, $threads_extract, $flm); exit 0;};
    $forks->{$pid} = "extract";
    $pid = fork();
    if (!$pid) {INFO "Starting forker process statistics.\n";focker_launcher(\&statistics_worker, \&statistics_getwork, $threads_stats); exit 0;};
    $forks->{$pid} = "statistics";
    $pid = fork();
    if (!$pid) {INFO "Starting forker process logparser.\n";focker_launcher(\&logparser_worker, \&logparser_getwork, $threads_log); exit 0;};
    $forks->{$pid} = "logs";
    $pid = fork();
    if (!$pid) {INFO "Starting forker process munin.\n";sleep 2;focker_launcher(\&munin_worker, \&munin_getwork, $threads_munin); exit 0;};
    $forks->{$pid} = "munin";
    $pid = fork();
    if (!$pid) {periodic_checks($forks, $main_pid); exit 0;};
    $forks->{$pid} = "checks";
    use Linux::Inotify2;

    DEBUG Dumper($forks);
    use Mind_work::SqlWork;

    my $dbh = new SqlWork();
    $dbh->clean_existing_files();
    $dbh->nulifyPluginsQueue();
#     addFilesDB ($_, $dbh) foreach (MindCommons::find_files_recursively($uploads_dir));
    assign_watchers($uploads_dir, $dbh);

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
		addFilesDB ($event->fullname, $dbh);
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
    my ($path, $dbh) = @_;
    my @files = MindCommons::find_files_recursively($path);
    foreach my $file (@files){
	if (-d $file && ! defined $watched_folders->{$file}) {
	    INFO "Watching new dir $file.\n";
	    $inotify->watch($file, IN_CREATE|IN_CLOSE_WRITE|IN_DELETE_SELF|IN_MOVED_FROM|IN_MOVED_TO, \&watch_handler);
	    $watched_folders->{$file} = 1;
	} elsif (-f $file) {
	    INFO "Found new file $file.\n";
	    addFilesDB ($file, $dbh);
	}
    }
}

eval {main_process_worker();};
ERROR "Error in main thread: $@\n" if $@;
kill 9, map {s/\s//g; $_} split /\n/, `ps -o pid --no-headers --ppid $$`;
exit ERRORS_LAST;
