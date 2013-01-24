<?php
  header('Pragma: no-cache');
  header('Cache-Control: no-cache, must-revalidate');
  header('Expires: Mon, 26 Jul 1997 05:00:00 GMT');
 header('Content-type: application/json');

// error_reporting(E_STRICT);

include_once('config.inc.php');
connect_db();

function get_plugins($customer, $hostname, $extra){
    $arr=array("plugins"=>get_plugins_array_sql(get_host_id_sql($customer, $hostname)));
    print json_encode( $arr );
}
function rebuild_plugin($customer, $hostname, $extra){
    $text = $extra->text;
    $plugin_id = $extra->id;
    $sample_rate = $extra->sample_rate;
    print "";
}

function get_hosts($customer, $hostname, $extra){
    $arr=get_hosts_sql($customer);
    print json_encode( $arr );
}

function get_plugin_text($customer, $hostname, $extra){
    list($text, $update_rate) = get_plugin_def_sql($extra->id);
    $arr=array("text"=>$text, "id"=>$extra->id, "update_rate"=>$update_rate);
    print json_encode( $arr );
}

function set_plugin_text($customer, $hostname, $extra){
    $text = $extra->text;
    $plugin_id = $extra->id;
    $sample_rate = $extra->sample_rate;
    set_plugin_def($plugin_id, $text, $sample_rate);
    print "";
}

function get_customers_autocomplete ($customer, $hostname, $extra){
    $arr = get_customers_autocomplete_sql($extra->request);
    print json_encode( $arr );
}

function get_customer_exists($customer, $hostname, $extra){
    print json_encode( customer_exists_sql($customer) );
}

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $post_data = json_decode($_POST['json']);
    $customer = $post_data->customer;
    $hostname = $post_data->hostname;
    $function = $post_data->data->function;
    $extra = $post_data->data->extra;
    if (function_exists($function) ) {
	call_user_func($function, $customer, $hostname, $extra);
    } else {
	error_log("function name '$function' doesn't exist.");
    }

//     error_log ($_POST['json']);
} else if ($_SERVER['REQUEST_METHOD'] === 'GET') {
    error_log ("get = ".implode("|",$_GET));
}
close_db();
?> 
