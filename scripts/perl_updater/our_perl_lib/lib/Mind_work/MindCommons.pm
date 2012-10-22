package MindCommons;

use warnings;
use strict;

use File::Path qw(make_path remove_tree);
# use Digest::MD5 qw(md5_hex);
use File::Basename;
use File::Copy;
use File::Find;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Log::Log4perl qw(:easy);

my $config = xmlfile_to_hash("config.xml");
use Definitions ':all';

sub get_random {
  return sprintf("%08X", rand(0xFFFFFFFF));
}

sub xmlfile_to_hash {
    my $file = shift;
    use XML::Simple;
    my $xml = new XML::Simple;
    return $xml->XMLin($file);
}

sub hash_to_xmlfile {
    my ($file, $hash, $root_name) = @_;
    use XML::Simple;
    $root_name = "out" if ! defined $root_name;
    my $xs = new XML::Simple();
    my $xml = $xs->XMLout($hash,
		    NoAttr => 1,
		    RootName=>$root_name,
		    OutputFile => $file
		    );
}

sub find_files_recursively {
    my $path = shift;
#     print "-Start searching for files in $path dir.\n";
    my @files;
    find(sub{push @files, $File::Find::name},$path);;
    return @files;
}

# sub normalize_text {
#     my $str = shift;
#     use Encode;
#     use Unicode::Normalize 'NFD','NFC','NFKD','NFKC';
#     ## from http://www.ahinea.com/en/tech/accented-translate.html
#     for ( $str ) {  # the variable we work on
# 	##  convert to Unicode first
# 	##  if your data comes in Latin-1, then uncomment:
# 	$_ = Encode::decode( 'utf8', $_ );
# 
# 	s/\xe4/ae/g;  ##  treat characters ä ñ ö ü ÿ
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
# 	s/\x{00df}/ss/g;  ##  German beta “ß” -> “ss”
# 	s/\x{00c6}/AE/g;  ##  Æ
# 	s/\x{00e6}/ae/g;  ##  æ
# 	s/\x{0132}/IJ/g;  ##  ?
# 	s/\x{0133}/ij/g;  ##  ?
# 	s/\x{0152}/Oe/g;  ##  Œ
# 	s/\x{0153}/oe/g;  ##  œ
# 
# 	tr/\x{00d0}\x{0110}\x{00f0}\x{0111}\x{0126}\x{0127}/DDddHh/; # ÐÐðdHh
# 	tr/\x{0131}\x{0138}\x{013f}\x{0141}\x{0140}\x{0142}/ikLLll/; # i??L?l
# 	tr/\x{014a}\x{0149}\x{014b}\x{00d8}\x{00f8}\x{017f}/NnnOos/; # ???Øø?
# 	tr/\x{00de}\x{0166}\x{00fe}\x{0167}/TTtt/;                   # ÞTþt
# 
# 	s/[^\0-\x80]//g;  ##  clear everything else; optional
#     }
#     return Encode::encode( 'utf8', $str );  ;
# }

sub get_file_sha {
    my $doc_file = shift;
    LOGDIE "Not a file: $doc_file\n" if ! -f $doc_file;
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

sub array_diff {
    TRACE "-Compute difference and uniqueness.\n";
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
    TRACE "\tdifference done.\n";

    my $arr1_hash = ();
    $arr1_hash->{$_} = 1 foreach (@$arr1);

    foreach my $element (@difference) {
	if (exists $arr1_hash->{$element}) {
	    push @only_in_arr1, $element;
	} else {
	    push @only_in_arr2, $element;
	}
    }
    TRACE "+Compute difference and uniqueness.\n";
    return \@only_in_arr1,  \@only_in_arr2,  \@intersection;
}

return 1;
