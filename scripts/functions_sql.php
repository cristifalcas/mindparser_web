<?php

$db_link;
$db_user = 'mind_statistics';
$db_pass = '!0mind_statistics@9';
$db_database = 'mind_statistics';
$customers_table = '__customers';
$hosts_table = '__hosts';
$plugins_table = '__mind_plugins';
$plugins_conf_table = '__mind_plugins_conf';
$md5_names_table = '__md5_col_names';

function connect_db() {
    global $db_link, $db_user, $db_pass, $db_database;
    $db_link = mysql_connect('localhost', $db_user, $db_pass);
    if (! ($db_link && mysql_select_db($db_database, $db_link))) {
	error_log('Could not connect: ' . mysql_error());
        exit;
    }
}

function close_db() {
    global $db_link;
    mysql_close($db_link);
}

function get_customers_sql() {
    global $customers_table;
    $customers = "";
    $query = "select * from $customers_table where id>0 order by name";
    $rs = mysql_query($query) or error_log("get_customers: $query || ".mysql_error());
    while($row = mysql_fetch_array($rs)){
	$customers[$row['id']] = $row['name'];
    }
    return $customers;
}

function get_customer_id_sql ($customer_name) {
    global $customers_table;
    $query = "select id from $customers_table where name='$customer_name'";
    $rs = mysql_query($query) or error_log("get_customer_id_sql: $query || ".mysql_error());
    $row = mysql_fetch_array($rs);
    return $row['id'];
}

// function get_customers_autocomplete_sql($str) {
//     global $customers_table;
//     $query = "select name from $customers_table where lower(name) like '".$str."%' order by 1"; // LIMIT 0, 10
//     $rs = mysql_query($query) or error_log("get_customers: $query || ".mysql_error());
//     $arr = array();
//     while($row = mysql_fetch_array($rs)){
// 	array_push($arr, $row['name']);
//     }
//     return $arr;
// }

function get_hosts_sql($customer) {
    global $hosts_table, $customers_table;
    $machines = array();
    $query = "select h.* from $hosts_table h, $customers_table c where h.customer_id=c.id and c.name='$customer' order by h.name";
    $rs = mysql_query($query) or error_log("get_hosts: $query || ".mysql_error());
    while($row = mysql_fetch_array($rs)){
	$machines[$row['id']] = $row['name'];
    }
    return $machines;
}

function get_host_id_sql ($customer_name, $host_name) {
    global $hosts_table, $customers_table;
    $query = "select h.id from $hosts_table h, $customers_table c where h.customer_id=c.id and c.name='$customer_name' and h.name='$host_name'";
    $rs = mysql_query($query) or error_log("validate: $query || ".mysql_error());
    $row = mysql_fetch_array($rs);
    return $row['id'];
}

function validate_customer_host_sql($customer_name, $host_name) {
    global $hosts_table, $customers_table;
    $query = "select count(*) as nr from $hosts_table h, $customers_table c where h.customer_id=c.id and c.name='$customer_name' and h.name='$host_name'";
    $rs = mysql_query($query) or error_log("validate: $query || ".mysql_error());
    $row = mysql_fetch_array($rs);
    return $row['nr']==1;
}

function customer_exists_sql($customer_name) {
    global $customers_table;
    $query = "select count(*) as nr from $customers_table where name='$customer_name'";
    $rs = mysql_query($query) or error_log("customer_exists_sql: $query || ".mysql_error());
    $row = mysql_fetch_array($rs);
    return $row['nr']==1;
}

function get_plugins_array_sql($host_id) {
    global $plugins_table;
    $plugins = array();
    $query = "select * from $plugins_table where host_id='$host_id'";
    $rs = mysql_query($query) or error_log("get_plugins_array: $query || ".mysql_error());
    while($row = mysql_fetch_array($rs)){
	array_push($plugins, ["id"=>$row['id'], "name"=>$row['plugin_name']]);
    }
    return $plugins;
}

function get_plugin_def_sql ($plugin_id){
    global $plugins_conf_table, $md5_names_table, $plugins_table;
    $query = "SELECT c.section_name,m.name FROM $plugins_conf_table c, $md5_names_table m where c.plugin_id=$plugin_id and c.md5_name=m.md5 order by 1,2";
    $rs = mysql_query($query) or error_log("get_plugin_def1: $query || ".mysql_error());
    $plugin_def_map = array();
    while($row = mysql_fetch_array($rs)){
	if (!isset($plugin_def_map[$row['section_name']])) $plugin_def_map[$row['section_name']] = array();
	array_push($plugin_def_map[$row['section_name']], $row['name']);
    }
    $str = "";
    foreach ($plugin_def_map as $key1 => $arr) {
// 	print "[$key1]</br>";
	$str .= "\n[$key1]\n";
	foreach ($arr as $key2 => $value) {
// 	    print "$value</br>";
	    $str .= "$value\n";
	}
    }
    
    $query = "SELECT update_rate FROM $plugins_table where id=$plugin_id";
    $rs = mysql_query($query) or error_log("get_plugin_def2: $query || ".mysql_error());
    $update_rate = mysql_fetch_array($rs);

    return array(substr($str, 1), $update_rate['update_rate']);
}

function set_plugin_def ($plugin_id, $text, $sample_rate){
    global $plugins_conf_table, $md5_names_table, $plugins_table;
    mysql_query("LOCK TABLES $plugins_conf_table WRITE, $md5_names_table WRITE;") or error_log("can't lock table $plugins_conf_table: ".mysql_error());
    $section = "Not configured";
    foreach(explode("\n", $text) as $value) {
	$value = preg_replace('/\s+/',' ', $value);
	if (preg_match("/^\[.*\]$/i", $value)){
	    $section = preg_replace("/^\[(.*)\]$/i", "$1", $value);
	} else {
	    $query = "select md5 from  $md5_names_table where name='$value'";
	    $rs = mysql_query($query) or error_log("set_plugin_def1: $query || ".mysql_error());
	    $md5_name = mysql_fetch_array($rs)['md5'];
	    $query = "update $plugins_conf_table set section_name='$section' where plugin_id=$plugin_id and md5_name='$md5_name' and section_name<>'$section'";
	    mysql_query($query) or error_log("set_plugin_def2: $query || ".mysql_error());
	    if (mysql_affected_rows() > 0){error_log("section plugin_id=$plugin_id and md5_name=$md5_name and section_name=$section");}
	}
    }
    mysql_query("UNLOCK TABLES;");
    
    $query = "update $plugins_table set update_rate=$sample_rate where id=$plugin_id";
    $result = mysql_query($query) or return_error($query, mysql_error()); 
}

function add_customer_sql ($customer){
    global $customers_table;
    $query = "INSERT IGNORE INTO $customers_table (name) VALUES('$customer')";
    $result = mysql_query($query) or return_error($query, mysql_error()); 
}

function delete_customer_sql ($customer){
    global $customers_table;
    $query = "delete from $customers_table where name='$customer'";
    $result = mysql_query($query) or return_error($query, mysql_error()); 
}

function add_host_sql ($customer, $host){
    global $hosts_table;
    $cust_id = get_customer_id_sql($customer);
    $query = "INSERT IGNORE INTO $hosts_table (customer_id, name) VALUES('$cust_id','$host')";
    $result = mysql_query($query) or return_error($query, mysql_error()); 
}

function delete_host_sql ($customer, $host){
    global $hosts_table, $customers_table;
    $query = "delete from $hosts_table where name='$host' and customer_id=(select id from $customers_table where name='$customer')";
    $result = mysql_query($query) or return_error($query, mysql_error()); 
}

function return_error($query, $mysql_error) {
    header("HTTP/1.1 500 Internal Server Error");
    $err = "Error in query $query.\n $mysql_error\n";
    error_log($err);
    print json_encode($err);
    die;
}
?> 
