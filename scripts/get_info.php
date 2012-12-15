<?php
  header('Pragma: no-cache');
  header('Cache-Control: no-cache, must-revalidate');
  header('Expires: Mon, 26 Jul 1997 05:00:00 GMT');
//   header('Content-type: text/xml');
 header('Content-type: application/json');

// error_reporting(E_STRICT);

// $customer="";
// $host="";

include_once('config.inc.php');
connect_db();

function get_plugins($customer, $hostname, $extra){
// 	    $arr = array("plugins"=>array(["id"=>12,	"name"=>"sg",		"text"=>"textes sdfg"],
// 					  ["id"=>234,	"name"=>"s245g",	"text"=>"textes 4235"],
// 					  ["id"=>245,	"name"=>"gf",		"text"=>"here comes jhonny "],
// 					  ["id"=>31154,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>353224,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3534,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>344,	"name"=>"fdsghefdsghefgsdffgsdf",	"text"=>"textes 54tssssssssssssssssfcert"],
// 					  ["id"=>3654,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3554,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3754,	"name"=>"fdsghfdsghefgsdfefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3584,	"name"=>"1223",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3954,	"name"=>"344",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3054,	"name"=>"edrfe",	"text"=>"textes 54tfcert"],
// 					  ["id"=>35114,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>31254,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3564324,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>35534,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3447,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>36548,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>35549,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>375411,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>358412,	"name"=>"fdsghfdsghefgsdffdsghefgsdfefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>395413,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>305441,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3511433,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>3512422,	"name"=>"fdsfdsghefgsdfghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>315444,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>352455,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>353456,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>34467,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>365466,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>355477,	"name"=>"fdsgheffdsghefgsdfgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>375478,	"name"=>"fdsghfdsghefgsdfefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>358488,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>395498,	"name"=>"fdsfdsghefgsdfghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>305499,	"name"=>"fdsgfdsghefgsdffdsghefgsdfhefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>35114111,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>351241111,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>35124222,	"name"=>"fdsghefgsdf",	"text"=>"textes 54tfcert"],
// 					  ["id"=>123,	"name"=>"q",		"text"=>"textes w"]));
    
    $arr=array("plugins"=>get_plugins_array(get_host_id($customer, $hostname)));
    print json_encode( $arr );
}
function rebuild_plugin($customer, $hostname, $extra){
    $text = $extra->text;
    $plugin_id = $extra->id;
    $sample_rate = $extra->sample_rate;
}

function get_plugin_text($customer, $hostname, $extra){
    list($text, $update_rate) = get_plugin_def($extra->id);
    $arr=array("text"=>$text, "id"=>$extra->id, "update_rate"=>$update_rate);
    print json_encode( $arr );
}

function set_plugin_text($customer, $hostname, $extra){
    $text = $extra->text;
    $plugin_id = $extra->id;
    $sample_rate = $extra->sample_rate;
    set_plugin_def($plugin_id, $text, $sample_rate);
//     error_log($plugin_id.$text);
}

function get_customers_autocomplete ($customer, $hostname, $extra){
//     $search_string = $extra->request;
//     $arr=array("foo", "bar", "hallo", "world");
    $arr = get_customers_autocomplete_sql($extra->request);
    print json_encode( $arr );
//     print '"foo", "bar", "hallo", "world"';
}

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $post_data = json_decode($_POST['json']);
    $customer = $post_data->customer;
    $hostname = $post_data->hostname;
    $function = $post_data->data->function;
    $extra = $post_data->data->extra;
    call_user_func($function, $customer, $hostname, $extra);

//     error_log ($_POST['json']);
} else if ($_SERVER['REQUEST_METHOD'] === 'GET') {
    error_log ("get = ".implode("|",$_GET));
}
close_db();
?> 
