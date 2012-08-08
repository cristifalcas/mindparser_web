package MindCommons;

use warnings;
use strict;

use File::Path qw(make_path remove_tree);
# use Unicode::Normalize 'NFD','NFC','NFKD','NFKC';
# use Digest::MD5 qw(md5_hex);
# use File::Basename;
# use File::Copy;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use XML::Simple;
use Encode;

my $debug = 0;

sub get_random {
  return sprintf("%08X", rand(0xFFFFFFFF));
}

sub xmlfile_to_hash {
    my $file = shift;
    my $xml = new XML::Simple;
    return $xml->XMLin("$file");
}

sub hash_to_xmlfile {
    my ($hash, $name, $root_name) = @_;
    $root_name = "out" if ! defined $root_name;
    my $xs = new XML::Simple();
    my $xml = $xs->XMLout($hash,
		    NoAttr => 1,
		    RootName=>$root_name,
		    OutputFile => $name
		    );
}

sub copy_dir {
    my ($from_dir, $to_dir) = @_;
    opendir my($dh), $from_dir or die "Could not open dir '$from_dir': $!";
    for my $entry (readdir $dh) {
#         next if $entry =~ /$regex/;
        my $source = "$from_dir/$entry";
        my $destination = "$to_dir/$entry";
        if (-d $source) {
	    next if $source =~ "\.?\.";
            mkdir $destination or die "mkdir '$destination' failed: $!" if not -e $destination;
            copy_dir($source, $destination);
        } else {
            copy($source, $destination) or die "copy failed: $source to $destination $!";
        }
    }
    closedir $dh;
    return;
}

sub move_dir {
    my ($src, $trg) = @_;
    die "\tTarget $trg is a file.\n" if (-f $trg);
    makedir("$trg", 1) if (! -e $trg);
    opendir(DIR, "$src") || die("Cannot open directory $src.\n");
    my @files = grep { (!/^\.\.?$/) } readdir(DIR);
    closedir(DIR);
    foreach my $file (@files){
	move("$src/$file", "$trg/$file") or die "Move file $src/$file to $trg failed: $!\n";
    }
    remove_tree("$src") || die "Can't remove dir $src.\n";
}

sub write_file {
    my ($path,$text, $remove) = @_;
    $remove = 0 if not defined $remove;
    my ($name,$dir,$suffix) = fileparse($path, qr/\.[^.]*/);
    add_to_remove("$dir/$name$suffix", "file") if $remove ne 0;
    print "\tWriting file $name$suffix.\t". get_time_diff() ."\n";
    open (FILE, ">$path") or die "at generic write can't open file $path for writing: $!\n";
    ### don't decode/encode to utf8
    print FILE "$text";
    close (FILE);
}

sub makedir {
    my ($dir, $no_extra) = @_;
    my ($name_user, $pass_user, $uid_user, $gid_user, $quota_user, $comment_user, $gcos_user, $dir_user, $shell_user, $expire_user) = getpwnam scalar getpwuid $<;
    my $err;
    if (defined $no_extra) {
	make_path ("$dir", {error => \$err});
    } else {
	make_path ("$dir", {owner=>"$name_user", group=>"nobody", error => \$err});
    }
    if (@$err) {
	for my $diag (@$err) {
	    my ($file, $message) = %$diag;
	    if ($file eq '') { print "general error: $message.\n"; }
	    else { print "problem unlinking $file: $message.\n"; }
	}
	die "Can't make dir $dir: $!.\n";
    }
    die "Dir not created.\n" if ! -d $dir;
}

# sub normalize_text {
#     my $str = shift;
#     ## from http://www.ahinea.com/en/tech/accented-translate.html
#     for ( $str ) {  # the variable we work on
# 	##  convert to Unicode first
# 	##  if your data comes in Latin-1, then uncomment:
# 	$_ = Encode::decode( 'utf8', $_ );
# 
# 	s/\xe4/ae/g;  ##  treat characters � � � � �
# 	s/\xf1/ny/g;  ##  this was wrong in previous version of this doc
# 	s/\xf6/oe/g;
# 	s/\xfc/ue/g;
# 	s/\xff/yu/g;
# 	## various apostrophes   http://www.mikezilla.com/exp0012.html
# 	s/\x{02B9}/\'/g;
# 	s/\x{2032}/\'/g;
# 	s/\x{0301}/\'/g;
# 	s/\x{02C8}/\'/g;
# 	s/\x{02BC}/\'/g;
# 	s/\x{2019}/\'/g;
# 
# 	$_ = NFD( $_ );   ##  decompose (Unicode Normalization Form D)
# 	s/\pM//g;         ##  strip combining characters
# 
# 	# additional normalizations:
# 
# 	s/\x{00df}/ss/g;  ##  German beta �ߔ -> �ss�
# 	s/\x{00c6}/AE/g;  ##  �
# 	s/\x{00e6}/ae/g;  ##  �
# 	s/\x{0132}/IJ/g;  ##  ?
# 	s/\x{0133}/ij/g;  ##  ?
# 	s/\x{0152}/Oe/g;  ##  �
# 	s/\x{0153}/oe/g;  ##  �
# 
# 	tr/\x{00d0}\x{0110}\x{00f0}\x{0111}\x{0126}\x{0127}/DDddHh/; # ���dHh
# 	tr/\x{0131}\x{0138}\x{013f}\x{0141}\x{0140}\x{0142}/ikLLll/; # i??L?l
# 	tr/\x{014a}\x{0149}\x{014b}\x{00d8}\x{00f8}\x{017f}/NnnOos/; # ???��?
# 	tr/\x{00de}\x{0166}\x{00fe}\x{0167}/TTtt/;                   # �T�t
# 
# 	s/[^\0-\x80]//g;  ##  clear everything else; optional
#     }
#     return Encode::encode( 'utf8', $str );  ;
# }

# sub get_string_md5 {
#     my $text = shift;
#     return md5_hex($text);
# }
# 
# sub get_file_md5 {
#     my $doc_file = shift;
#     open(FILE, $doc_file) or die "Can't open '$doc_file': $!\n";
#     binmode(FILE);
#     my $doc_md5 = Digest::MD5->new->addfile(*FILE)->hexdigest;
#     close(FILE);
#     return $doc_md5;
# }

sub get_file_sha {
    my $doc_file = shift;
    use Digest::SHA qw(sha1_hex);
    my $sha = Digest::SHA->new();
    $sha->addfile($doc_file);
    return $sha->hexdigest;;
}

sub get_string_sha {
    my $text = shift;
    use Digest::SHA qw(sha1_hex);
    return sha1_hex($text);
}

sub capitalize_string {
    my ($str,$type) = @_;
    if ($type eq "first") {
	$str =~ s/\b(\w)/\U$1/g;
    } elsif ($type eq "all") {
	$str =~ s/([\w']+)/\u\L$1/g;
    } elsif ($type eq "small") {
	$str =~ s/([\w']+)/\L$1/g;
    } elsif ($type eq "onlyfirst") {
	$str =~ s/\b(\w)/\U$1/;
    } else {
	die "Capitalization: first (first letter is capital and the rest remain the same), small (all letters to lowercase) or all (only first letter is capital, and the rest are lowercase).\n";
    }
    return $str;
}

sub array_diff {
    print "-Compute difference and uniqueness.\n" if $debug;
    my ($arr1, $arr2) = @_;
    my %seen = (); my @uniq1 = grep { ! $seen{$_} ++ } @$arr1; $arr1 = \@uniq1;
    %seen = (); my @uniq2 = grep { ! $seen{$_} ++ } @$arr2; $arr2 = \@uniq2;

    my (@only_in_arr1, @only_in_arr2, @common) = ();
## union: all, intersection: common, difference: unique in a and b
    my (@union, @intersection, @difference) = ();
    my %count = ();
    foreach my $element (@$arr1, @$arr2) { $count{"$element"}++ }
    foreach my $element (sort keys %count) {
	push @union, $element;
	push @{ $count{$element} > 1 ? \@intersection : \@difference }, $element;
# 	push @difference, $element if $count{$element} <= 1;
    }
    print "\tdifference done.\n" if $debug;

    my $arr1_hash = ();
    $arr1_hash->{$_} = 1 foreach (@$arr1);

    foreach my $element (@difference) {
	if (exists $arr1_hash->{$element}) {
	    push @only_in_arr1, $element;
	} else {
	    push @only_in_arr2, $element;
	}
    }
    print "+Compute difference and uniqueness.\n" if $debug;
    return \@only_in_arr1,  \@only_in_arr2,  \@intersection;
}

return 1;
