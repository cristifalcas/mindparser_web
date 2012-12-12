<?php
  header('Pragma: no-cache');
  header('Cache-Control: no-cache, must-revalidate');
  header('Expires: Mon, 26 Jul 1997 05:00:00 GMT');
//   header('Content-type: text/xml');
 header('Content-type: application/json');

// error_reporting(E_STRICT);

$customer="";
$host="";

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

function get_plugin_text($customer, $hostname, $extra){
    $text = get_plugin_def($extra->id);
    $arr=array("text"=>$text, "id"=>$extra->id);
    print json_encode( $arr );
}

function set_plugin_text($customer, $hostname, $extra){
//     $text = get_plugin_def($extra->id);
//     $arr=array("text"=>$text, "id"=>$extra->id);
//     print json_encode( $arr );
    error_log(print_r($extra,true));
}

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $post_data = json_decode($_POST['json']);
    $customer = $post_data->customer;
    $hostname = $post_data->hostname;
//     $data = $post_data->data;
    $function = $post_data->data->function;
    $extra = $post_data->data->extra;
    call_user_func($function, $customer, $hostname, $extra);

    error_log ("post = $customer $hostname $function ");
} else if ($_SERVER['REQUEST_METHOD'] === 'GET') {
    error_log ("get = ".implode("|",$_POST));
}

?> 
