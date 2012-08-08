package SqlWork;

use warnings;
use strict;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use DBI;
use Log::Log4perl qw(:easy);

use Mind_work::MindCommons;
my ($db_database, $db_user, $db_pass, 
  $cust_table, $host_table, $collected_file_table, $md5_names_table, $stats_template_table, 
  $db_h);

Log::Log4perl->easy_init({ level   => $INFO,
#                            file    => ">>test.log" 
			   layout   => "%d [%5p] (%6P) %m%n",
});

sub new {
    my $class = shift;
    my $config = MindCommons::xmlfile_to_hash("config.xml");
    $db_database = $config->{db_config}->{db_database};
    $db_user = $config->{db_config}->{db_user};
    $db_pass = $config->{db_config}->{db_pass};
    $cust_table = $config->{db_config}->{cust_table};
    $host_table = $config->{db_config}->{host_table};
    $collected_file_table = $config->{db_config}->{collected_file_table};
    $md5_names_table = $config->{db_config}->{md5_names_table};
    $stats_template_table = $config->{db_config}->{stats_template_table};
    
    my $self = { };
    $db_h = DBI->connect("DBI:mysql:$db_database", $db_user, $db_pass,
	{ ShowErrorStatement => 1,
          AutoCommit => 1,
          RaiseError => 1,
          mysql_use_result => 0,
          mysql_enable_utf8 => 1,
          PrintError => 1, }) || die "Could not connect to database: $DBI::errstr";
    bless($self, $class);
    return $self;
}

sub getDBI_handler {
    return $db_h;
}

sub updateFileStatus {
    my ($self, $fileid, $status) = @_;
    my $sth = $db_h->do("update $collected_file_table set status=$status WHERE id=$fileid") || die "Error $DBI::errstr\n";
}

sub updateFileInfo {
    my ($self, $fileid, $duration, $to_table_name) = @_;
    my $sth = $db_h->do("update $collected_file_table set 
	      parse_duration=$duration,
	      parse_done_time=NOW(),
	      inserted_in_tablename='$to_table_name'
	  WHERE id=$fileid") || die "Error $DBI::errstr\n";
}

sub createStatsTable {
    my ($self, $table_name) = @_;
    my $query = $db_h->do("CREATE TABLE IF NOT EXISTS $table_name LIKE $stats_template_table") || die "Error $DBI::errstr\n";
    $db_h->do("ALTER TABLE $table_name add FOREIGN KEY (host_id) REFERENCES hosts(id)") || die "Error $DBI::errstr\n" if ! defined $query;
}

sub cloneForFork {
#     http://www.perlmonks.org/?node_id=594175
    my $self = shift;
    my $child_dbh = $db_h->clone();
    $db_h->{InactiveDestroy} = 1;
    undef $db_h;
    $db_h = $child_dbh;
}

sub getFile {
    my ($self, $id) = @_;
    my $hash_ref = $db_h->selectrow_hashref("SELECT * FROM $collected_file_table WHERE id=$id");
    return $hash_ref;
}

sub getColumnList {
  my($self, $table) = @_;
  
  my $sth = $db_h->prepare("SELECT * FROM $table WHERE 1=0");
  $sth->execute || die "Error $DBI::errstr\n";
  my @cols = @{$sth->{NAME}}; # or NAME_lc if needed
  $sth->finish;
  return \@cols;
}

sub getCustomers {
    my $self = shift;
    my $hash;
    my $query = $db_h->prepare("select * from $cust_table");
    $query->execute() || die "Error $DBI::errstr\n";
    while (my @row = $query->fetchrow_array ){
	die Dumper(@row)."lame 1\n" if @row != 2;
	my $query_h = $db_h->prepare("select * from $host_table where customer_id=$row[0]");
	$query_h->execute() || die "Error $DBI::errstr\n";
	while (my @row_h = $query_h->fetchrow_array ){
	    die Dumper(@row_h)."lame 2\n" if @row_h != 4;
	    $hash->{$row[1]}->{'id'} = $row[0];
	    $hash->{$row[1]}->{'hosts'}->{$row_h[2]} = $row_h[0];
	}
    }
    return $hash;
}

# start work if stats file done and no other stats pending. thread should be per host+stats type
sub getWorkForMunin {
    my ($self, $status) = @_;
    my $hash = $db_h->selectall_hashref("select * from $collected_file_table where status=$status or status=0 and inserted_in_tablename is not null", ['host_id', 'inserted_in_tablename', 'status', 'id']);
# print Dumper($hash);
    my ($ret, $checker);
    foreach my $h_id (keys %$hash){
	my $host_tables = $hash->{$h_id};
	foreach my $table (keys %$host_tables){
	    my $files_id = $host_tables->{$table}->{$status};
	    foreach my $f_id (keys %$files_id) {
		LOGDIE "We have the same file id=$f_id to different hosts.".Dumper($checker) if defined $checker->{$f_id};
		$checker->{$f_id} = $h_id;
	    }
	    next if defined $host_tables->{$table}->{0} || ! defined $host_tables->{$table}->{$status};
	    $ret->{"$h_id:$table"} = [keys %$files_id];
	}
    }
# print Dumper($ret);
    return $ret;
}

sub getFilesForParsers {
    my ($self) = @_;
    my $hash = $db_h->selectall_hashref("select * from $collected_file_table where status=0", 'id');
    return $hash;
}

sub insertFile() {
    my ($self, $customer_id, $host_id, $file_name, $file_size, $status) = @_;
    return if ! -f $file_name;
    my $md5 = MindCommons::get_file_sha($file_name);
    DEBUG "Inserting file $file_name.\n";
    my $query = "INSERT INTO $collected_file_table 
	    (customer_id,
	     host_id,
	     file_name,
	     file_md5,
	     size,
	     status)
	VALUES (
	    $customer_id, 
	    $host_id, 
	    ".$db_h->quote($file_name).", 
	    ".$db_h->quote($md5).", 
	    $file_size,
	    $status)";
    my $query_handle = $db_h->prepare($query);
    $query_handle->execute() || die "Error $DBI::errstr\n" || die "Error $DBI::errstr\n";
}

sub get_md5_names {
    my ($self, @arr) = @_;
    my @res;
    my $query = $db_h->prepare("SELECT * FROM $md5_names_table");
    $query->execute() || die "Error $DBI::errstr\n";
    my %hash = map{$_ => 1} @arr;
    my $res;
    while (my @row = $query->fetchrow_array){
	$res->{$row[1]} = $row[0] if defined $hash{$row[1]};
    }
    return $res;
} 

sub get_customer_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select * from customers where id=$id");
    return $arr_ref->[1];
}

sub get_host_name {
    my ($self, $id) = @_;
    my $arr_ref = $db_h->selectrow_arrayref("select * from hosts where id=$id");
    return $arr_ref->[2];
}

sub add_new_columns {
    my ($self, $table_name, $arr) = @_;
    DEBUG "Acquiring lock\n";
    $db_h->do("LOCK TABLES $table_name WRITE, $md5_names_table WRITE");
    my $columns_e = getColumnList($self, $table_name);
    my $md5_columns = ['id', 'file_id', 'timestamp', map{"x_".MindCommons::get_string_sha($_)} @$arr];
    my ($only_in_arr1, $only_in_arr2, $intersection) = MindCommons::array_diff( $md5_columns, $columns_e);
    while (my ($index, $md5) = each @$only_in_arr1) {
	INFO "Adding new column $md5 for table $table_name\n";
	$db_h->do("ALTER TABLE $table_name ADD $md5 decimal(15,5)");
	$db_h->do("INSERT IGNORE INTO $md5_names_table (md5, name) VALUES ('$md5', '$arr->[$index]')");
    }
    DEBUG "Releasing lock\n";
    $db_h->do("UNLOCK TABLES");
}

sub insertRowsDB {
    my ($self, $table_name, $col, $val) = @_;
    $db_h->do("INSERT IGNORE INTO $table_name $col VALUES $val") || die "Error $DBI::errstr\n";
}

sub disconnect {
    $db_h->disconnect(); 
}

return 1;
