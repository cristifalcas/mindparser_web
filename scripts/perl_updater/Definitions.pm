package Definitions;

use strict;
use warnings;

use base 'Exporter';

use constant {
    EXIT_STATUS_NA	=>-1,
    IGNORE		=> 0,
    START_EXTRACT	=> 1,	## file is ready for extract
    START_STATS		=> 2,	## file is ready for statistics gathering
    START_MUNIN		=> 3,	## file is ready for munin update
    START_PARSELOG	=> 4,	## file is ready for logs parsing

    SUCCESS_LAST	=> 99,
    ERRORS_START	=> 100,
    EXIT_NO_FILE	=> 101,
    EXIT_WRONG_TYPE	=> 102,
    EXIT_WRONG_NAME	=> 103,
    EXIT_EMPTY		=> 104,

    EXIT_WRONG_MIME	=> 120,
    EXIT_HOST_DELETE	=> 121,
    EXIT_EXTR_ERR	=> 122,
    EXIT_NO_LINES	=> 123,
    EXIT_NO_ROWS	=> 124,
    EXIT_NO_RRD		=> 125,
    EXIT_FILE_BAD	=> 126,
    EXIT_MUNIN_ERROR	=> 127,
    ERRORS_LAST		=> 1000,
};

# ^\s+ => '		\s+=(.*)\n => ', 
our @EXPORT_OK = ('EXIT_STATUS_NA', 'IGNORE', 'START_EXTRACT', 'START_STATS', 'START_MUNIN', 'START_PARSELOG', 'SUCCESS_LAST', 'ERRORS_START', 'EXIT_NO_FILE', 'EXIT_WRONG_TYPE', 'EXIT_WRONG_NAME', 'EXIT_EMPTY', 'EXIT_WRONG_MIME', 'EXIT_HOST_DELETE', 'EXIT_EXTR_ERR', 'EXIT_NO_ROWS', 'EXIT_NO_RRD', 'EXIT_FILE_BAD', 'EXIT_NO_LINES', 'EXIT_MUNIN_ERROR', 'ERRORS_LAST', );
our %EXPORT_TAGS = ( all => \@EXPORT_OK );

1;
