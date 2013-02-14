package Definitions;

use strict;
use warnings;

use base 'Exporter';

use constant {
    EXIT_STATUS_NA	=>-1,
    IGNORE		=> 0,
    START_EXTRACT	=> 10,	## file is ready for extract
    START_PARSERS	=> 20,	## file is ready for statistics gathering or logs parsing
    START_MUNIN		=> 30,	## file is ready for munin update

    SUCCESS_LAST	=> 99,
    ERRORS_START	=> 100,
    EXIT_NO_FILE	=> 101,
    EXIT_WRONG_TYPE	=> 102,
    EXIT_WRONG_NAME	=> 103,
    EXIT_EMPTY		=> 104,
    EXIT_ERROR_EXTRACT	=> 110,

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

our $stats_default_info = {
	__template__ => { group_by	=> [""],# default none
		 multi_value	=> [";"],	# default=;
		 delim		=> ",",		# default=,
		},
	rts => { group_by	=> ["GwIP"],
		 multi_value	=> [";"],
		 delim		=> ",",
		},
	dialogicopensessions => { group_by	=> ["SIU ID"]
		},
};

our $columns_header = ['file_id', 'host_id', 'timestamp', 'group_by'];

our @EXPORT_OK = qw(EXIT_STATUS_NA IGNORE START_EXTRACT START_PARSERS START_MUNIN SUCCESS_LAST ERRORS_START EXIT_NO_FILE EXIT_WRONG_TYPE EXIT_WRONG_NAME EXIT_EMPTY EXIT_ERROR_EXTRACT EXIT_WRONG_MIME EXIT_HOST_DELETE EXIT_EXTR_ERR EXIT_NO_LINES EXIT_NO_ROWS EXIT_NO_RRD EXIT_FILE_BAD EXIT_MUNIN_ERROR ERRORS_LAST $stats_default_info $columns_header);
our %EXPORT_TAGS = ( all => \@EXPORT_OK );

1;
