#!/usr/bin/perl -w

use warnings;
use strict;
$| = 1;
$SIG{__WARN__} = sub { die @_ };

use File::Find;
use File::Basename;
# use File::Slurp;
use Cwd 'abs_path';
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Time::Local;

my $script_path = (fileparse(abs_path($0), qr/\.[^.]*/))[1]."";
my $file = shift;
my $type = shift;

# my $text;
open (FILE, $file) or die "can't open file $file: $!\n"; 
# $text .= $_ foreach (<FILE>);
my @lines = <FILE>;
close (FILE); 

if ($type eq "iostat") {
    mod_iostat(@lines);
} elsif ($type eq "tomcat") {
    mod_tomcat(@lines);
}

sub tomcat_clean {
    my @lines = @_;
    my $text = "";
#     foreach (@lines){if ((length $_) > 350){$_ = substr($_, 350)} };
    $text .= $_ foreach (@lines);
    $text =~ s/^(.+?)(\d{4}-\d{2}-\d\d\s+\d{2}:\d\d:\d\d\s*.*)$/$1\n$2/gm;
    $text =~ s/^(.+?)(\d{4}-\d{2}-\d\d\s+\d{2}:\d\d:\d\d\s*.*)$/$1\n$2/gm;
    $text =~ s/^(.+?)(\d{4}-\d{2}-\d\d\s+\d{2}:\d\d:\d\d\s*.*)$/$1\n$2/gm;
    $text =~ s/^(.*)(memory.*)(memory.*)$/$1$2/gm;
#     $text =~ s/(\d{4}-\d{2}-\d\d)\s+(\d{2}:\d\d:\d\d)(\s*)\n(\s+)memory/$1 $2$3$4memory/gm;
#     $text =~ s/(\d{4}-\d{2}-\d\d)\s+(\d{2}:\d\d:\d\d)\s+(\d{4}-\d{2}-\d\d)\s+(\d{2}:\d\d:\d\d)(\s+)memory/$1 $2$5memory/gm;
#     $text =~ s/(\d{4}-\d{2}-\d\d\s+\d{2}:\d\d:\d\d\s+memory.*)(2012-\d{2}-\d\d\s+\d{2}:\d\d:\d\d\s+)/$1\n$2/gm;
#     $text =~ s/(\d{4}-\d{2}-\d\d)\s+(\d{2}:\d\d:\d\d)\s+(\d{4}-\d{2}-\d\d)\s+(\d{2}:\d\d:\d\d)(\s+)memory/$1 $2$5memory/gm;
    $text =~ s/(\d{4}-\d{2}-\d\d)\s+(\d{2}:\d\d:\d\d)(\s*)\n(\s+)memory/$1 $2$3$4memory/gm;
    $text =~ s/^\s*(memory.*)$//gm;
    $text =~ s/^\d{4}-\d{2}-\d\d\s+\d{2}:\d\d:\d\d\s*$//gms;
    $text =~ s/\s+$//gms;
#     open (FILE, ">$file.clean") or die "can't open file $file.clean: $!\n"; 
#     print FILE $text;
#     close FILE;
    return split /\n/, $text;
}

sub mod_tomcat {
    my @lines = @_;
    @lines = tomcat_clean(@lines);
# print Dumper(@lines);exit 1;
    my $once = 0;
    open (FILE, ">$file.done") or die "can't open file $file.done: $!\n"; 
    print FILE "Date, Time, memory_free, memory_total, memory_max, threadInfo_maxThread, threadInfo_minSpareThreads, threadInfo_maxSpareThreads, threadInfo_currentThreadCount, threadInfo_currentThreadsBusy=, requestInfo_maxTime, requestInfo_processingTime, requestInfo_requestCount, requestInfo_errorCount, requestInfo_bytesReceived, requestInfo_bytesSent\n";
    foreach my $line (@lines){
	next if $line =~m/^\s*$/;
# 	if ($line =~ m/\s*(\d{4}-\d{2}-\d\d)\s+(\d{2}:\d\d:\d\d)\s+(.*)$/) {
# 	}
	my @q = split m/\s+/, $line;
if (scalar @q != 19){print "Unknown line: $line\n".Dumper(@q);next;};
	my $date = shift @q;
	die "Unknown line date: $line\n".Dumper($date, @q) if ! defined $date;
	my ($year, $mon, $day) = split m/-/, $date;
	$year -=2000;
	my $time = shift @q;
	die "unknown line memory: $line\n" if shift @q ne "memory";
	my $free_mem = shift @q; $free_mem =~ s/^\[?free='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $total_mem = shift @q; $total_mem =~ s/^total='(\[?[0-9\.]+)'\*?[,\]]?$/$1/;
	my $max_mem = shift @q; $max_mem =~ s/^max='(\[?[0-9\.]+)'\*?[,\]]?$/$1/;
# print Dumper(@q);print "\n";
	die "unknown line threadInfo: $line\n" if shift @q ne "threadInfo";
	my $maxThreads = shift @q; $maxThreads =~ s/^\[?maxThreads='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $minSpareThreads = shift @q; $minSpareThreads =~ s/^\[?minSpareThreads='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $maxSpareThreads = shift @q; $maxSpareThreads =~ s/^\[?maxSpareThreads='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $currentThreadCount = shift @q; $currentThreadCount =~ s/^\[?currentThreadCount='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $currentThreadsBusy = shift @q; $currentThreadsBusy =~ s/^\[?currentThreadsBusy='([0-9\.]+)'\*?[,\]]?$/$1/;
	die "unknown line requestInfo: $line\n" if shift @q ne "requestInfo";
	my $maxTime = shift @q; $maxTime =~ s/^\[?maxTime='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $processingTime = shift @q; $processingTime =~ s/^\[?processingTime='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $requestCount = shift @q; $requestCount =~ s/^\[?requestCount='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $errorCount = shift @q; $errorCount =~ s/^\[?errorCount='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $bytesReceived = shift @q; $bytesReceived =~ s/^\[?bytesReceived='([0-9\.]+)'\*?[,\]]?$/$1/;
	my $bytesSent = shift @q; $bytesSent =~ s/^\[?bytesSent='([0-9\.]+)'\*?[,\]]?$/$1/;
	die "unknown line scalar: $line\n" if scalar @q;
	foreach ($free_mem, $total_mem, $max_mem, $maxThreads, $minSpareThreads, $maxSpareThreads, $currentThreadCount, $currentThreadsBusy, $maxTime, $processingTime, $requestCount, $errorCount, $bytesReceived, $bytesSent){
	    die "unknown line all $_: $line\n".Dumper($free_mem, $total_mem, $max_mem, $maxThreads, $minSpareThreads, $maxSpareThreads, $currentThreadCount, $currentThreadsBusy, $maxTime, $processingTime, $requestCount, $errorCount, $bytesReceived, $bytesSent) if $_ =~ m/[^0-9\.]/;
	}
	print FILE "$day/$mon/$year, $time, $free_mem, $total_mem, $max_mem, $maxThreads, $minSpareThreads, $maxSpareThreads, $currentThreadCount, $currentThreadsBusy, $maxTime, $processingTime, $requestCount, $errorCount, $bytesReceived, $bytesSent\n";
    }
    close (FILE);
}

sub getMonth {
    my $month = shift;
    my $m;
    if ( $month eq "Jan" ) { $m = 0 }
    elsif ( $month eq "Feb" ) { $m = 1 }
    elsif ( $month eq "Mar" ) { $m = 2 }
    elsif ( $month eq "Apr" ) { $m = 3 }
    elsif ( $month eq "May" ) { $m = 4 }
    elsif ( $month eq "Jun" ) { $m = 5 }
    elsif ( $month eq "Jul" ) { $m = 6 }
    elsif ( $month eq "Aug" ) { $m = 7 }
    elsif ( $month eq "Sep" ) { $m = 8 }
    elsif ( $month eq "Oct" ) { $m = 9 }
    elsif ( $month eq "Nov" ) { $m = 10 }
    elsif ( $month eq "Dec" ) { $m = 11 };
    return $m;
}

sub parse_iostat {
    my @block = @_;
    my $ext_dev_stat = shift @block;
    die "unknown block: \n".Dumper(@block) if $ext_dev_stat ne "extended device statistics";
    my @stats_names = split " ", shift @block;
    my $device = pop @stats_names;
    die "unknown block: \n".Dumper(@block) if $device ne "device";
    my $statistics;
    my $stats_header;
    my $done_once = 0;

    foreach my $line (@block) {
	my @stats_values = split " ", $line;
	my $stats_hash;
	foreach my $stat (@stats_names) {
	    my $val = shift @stats_values;
	    die "unknown block: \n".Dumper(@block) if ! defined $val;
	    $stats_hash->{$stat} = $val+0;
	    $stats_header->{$stat} = 1 if ! $done_once;
	    die "unknown block: \n".Dumper(@block) if ! defined $stats_header->{$stat} && $done_once;
	}
	$done_once++;
	my $device_name = join " ", @stats_values;
	die "unknown block: \n".Dumper(@block) if ! defined $device_name;
	$statistics->{$device_name} = $stats_hash;
    }
    return ($statistics, $stats_header);
}

sub mod_iostat {
    my @lines = @_;
    my ($header_count, $val_count);
    my $devices;
    my @block;
    my $once = 0;
    open (FILE, ">$file.done") or die "can't open file $file.done: $!\n"; 
    foreach my $line (@lines){
	next if $line =~ m/^\s*$/;
	$line =~ s/(^[\s\r]*)|([\s\r]*$)//g;
	$line =~ s/\s{2,}/ /g;
	if ($line =~ m/^\s*([a-z]+)\s+([a-z]+)\s+(\d{1,2})\s+(\d\d:\d\d:\d\d)\s+(\d+)\s*$/i){
	    my ($wd, $mon, $day, $time, $year) = ($1,getMonth($2)+1,$3,$4,$5-2000);
	    $mon = "0$mon" if($mon<10);
	    $day = "0$day" if($day<10);
	    my ($hour, $min, $sec) = split ":",$time;
# 	    my $unixtime = timelocal ($sec, $min, $hour, $day, $mon, $year);
	    if (scalar @block) {
		my $header = "Date, Time";
		my $value = "$day/$mon/$year, $hour:$min:$sec";
		my ($statistics, $stats_header) = parse_iostat(@block);
		$devices->{$_} = 1 foreach (keys %$statistics);  ## next blocks may not have all devices
		foreach my $device (sort keys %$devices) {
		    foreach my $stat (keys %$stats_header){
			$value .= ", ". ($statistics->{$device}->{$stat} || 0);
			$header .= ", $stat $device";
			delete $statistics->{$device}->{$stat};
		    }
		    die "cocoroco: \n".Dumper($statistics->{$device}) if scalar (keys %{$statistics->{$device}});
		}
# 		($header_count, $val_count) = (scalar )
		print FILE "$header\n" if ! $once++;
		print FILE "$value\n";
# 		$header_all = $header;
# print Dumper($header_all,$value);exit 1;
	    }
	    @block = ();
	} else {
	    push @block, $line;
	}
    }
    close (FILE);
}
