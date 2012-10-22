package SqlWork;

use warnings;
use strict;
$| = 1;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use Definitions ':all';

sub new {
    my $class = shift;
    my $self = { };

    bless($self, $class);
    return $self;
} 

return 1; 
