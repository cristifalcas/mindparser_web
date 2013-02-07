<?php
include_once('functions_sql.php');
$customer="";
$host="";

function connect() {
  connect_db();
}

function disconnect() {
  close_db();
}
// session_start();

// function make_dirs() {
//     foreach (get_customers_sql() as $cust) {
// 	foreach (get_customer_hosts_sql($cust) as $host) {
// 	    ## upload script is in another directory
// 	    $dir_full_path = dirname($_SERVER['SCRIPT_FILENAME'])."/scripts/uploads/$cust/$host/";
// 	    if (! is_dir ($dir_full_path)) {
// 		umask(0);
// 		mkdir($dir_full_path, 0775, true);
// 		chgrp($dir_full_path, "nobody");
//     // 	    copy("scripts/uploads.htaccess", "$dir_full_path/.htaccess");
// 	    }
// 	}
//     }
// }

// function generateSelect($name = '', $array = array(), $crt_selection, $input_array = array()) {
//     $other_name = "";
//     $other_value = "";
//     if (sizeof($input_array) == 2){
// 	$other_name = $input_array['name'];
// 	$other_value = $input_array['value'];
//     }
//     $html = "\n<form method=\"POST\">
// <input type=\"hidden\" name=\"$other_name\" value=\"$other_value\">
//     <select id=\"customers\" name=\"".$name."\" onchange=\"this.form.submit();\">
//       <option value=\"nada\">==Choose existing==</option>";
//     foreach ($array as $id => $value) {
// 	$selected="";
// 	if (!is_null($crt_selection) && $value == $crt_selection) {$selected=" selected=\"selected\" ";}
// 	$html .= "<option id=$id value=".$value."$selected>".$value."</option>";
//     }
// 
//     $html .= "</select></form>";
//     return $html;
// }

function generateStatusbar() {
    return '<div id="progress_container"><div id="progress_bar" style="width: 0%"></div></div>';;
}

function generateUpload($customer, $host) {
    $body="";
    $url="../cgi-bin/munin-cgi-html/$customer/index.html";

    $file_handle = fopen("scripts/load_body.html", "r");
    while (!feof($file_handle)) {
      $body .= fgets($file_handle);
    }
    fclose($file_handle);
    $body = str_replace(array('$$customer$$','$$host$$', '$$url$$'), array($customer,$host, $url), $body);

    $body .= "
	<div id=\"local_config\" customer=\"".$customer."\" hostname=\"".$host."\" view_mode=\"view\">
	</div>\n";
    return $body;
}

// function generatePluginEditButton($plugin_id) {
//     return '<form method="post" action="qq.php" id="target" class="'.$plugin_id.'">
// <div id="dialog-form" title="Edit some stats" class="dialog_plugin_'.$plugin_id.'">
//         <textarea id="textarea_edit_plugin" class="text ui-widget-content ui-corner-all" >test</textarea>
// </div>
// </form>
// <a class="link__plugin_'.$plugin_id.'">HTML Tutorial '.$plugin_id.'</a>
// ';
// // <button id="create-user" class="some_shit">Some stats</button>
// }
/*
function edit_customer_div(){
  $html = '
<div id="edit_customers" title="Edit customers">
    <fieldset class="myfields">
      <div class="inputdata">
	  <label>Customer: </label>
	  <input type="text" id="autocomplete_customers" class="defaultText" title="Enter customer"/>
	  <a class="select_customer noselect">&nbsp;</a>
      </div>
    </fieldset>
    <div class="cust_buttons">
        <a class="add_customer noselect">Select/Add customer</a>
        <a class="add_host noselect">Add new host</a>
    </div>
</div>';
  return $html;
}*/

function generateMenuInTable() {
    $html = '<table  class="tableMain">
  <tr>
    <td width=185 class="my_selector">
      <input type="image" src="img/graph_mode.jpg" name="image" class="switch_selector" title="Switch to edit mode">
    </td>
    <td >
	<div id="edit_plugins_forms_placer" style="display:none;"></div> 
	<div class="selector" style="width:100%; height:60px;overflow:auto;valign:top;" title="Edit plugin">
	    <ul id="change_edit_plugins" class="links_plugins"></ul>
	</div>
	<div class="selector" style="width:100%; height:60px;overflow:auto;valign:top;display:none;" title="View plugin">
	    <ul id="change_view_plugins" class="links_plugins"></ul>
	</div>
    </td>
  </tr>
</table><br/>
<div id="errors">Errors</div>
';
    return $html;
}

// function generateMenu() {
//     $html = '<form name="form_menu" method="get" action="index.php"> 
// <input type="hidden" name="menu_selection" value="" />
// <ul class="menu" id="menu">
// 	<li><a class="menulink noselect">Select Customer</a>
// 		<ul>';
//     $all_customers = get_customers_sql();
// //     sort($all_customers);
//     foreach ($all_customers as $cust) {
// 	$all_hosts = get_customer_hosts_sql($cust);
// 
// // 	if( !sizeof($all_hosts)){ continue;};
// 	$html .= "
// 			<li><a class=\"sub\">$cust</a>
// 				<ul>";
// 	$first = ' class="topline"';
// 	foreach ($all_hosts as $host) {
// 	    $html .= "
// 				    <li$first><a href=\"#\" onclick=\"document.form_menu.menu_selection.value='$cust,$host';document.form_menu.submit();\" >$host</a></li>";
// 	    $first = "";
// 	}
// 	$html .= "
// 				</ul>
// 			</li>";
//     }
//     $html .= '
// 		</ul>
// 	</li>
// </ul> 
// </form>
// <script type="text/javascript">	var menu=new menu.dd("menu");menu.init("menu","menuhover");</script>';
// // <br/><br/><br/>
//     return $html;
// }

function generateAddCustomer() {
  return '
<div id="edit_customer" title="Edit customer">
    <label>Customer: </label>
    <input type="text" class="defaultText" title="Enter customer name"/>
    <br/> <br/>
    <table id="hosts" class="ui-widget ui-widget-content">
	<thead>
	    <tr class="ui-widget-header ">
		<th>Host name</th>
		<th>IP</th>
		<th>User</th>
		<th>Password</th>
	    </tr>
	</thead>
	<tbody>
	    <tr id="add_new_host">
		<td><input type="text" class="defaultText ui-widget-content ui-corner-all" title="host name"/></td>
		<td><input type="text" class="defaultText ui-widget-content ui-corner-all" title="ip addr"/></td>
		<td><input type="text" class="defaultText ui-widget-content ui-corner-all" title="user"/></td>
		<td><input type="text" class="defaultText ui-widget-content ui-corner-all" title="pass"/></td>
	    </tr>
	</tbody>
    </table>
    <br/>
    <p>* Only customer and host names are required: a-z0-9_.</p>
</div>
<button class="add_customer_btn">Add new customer</button>';
}

function generate_customers() {
    $customers = get_customers_sql();
    $html = '<div id="accordion">';
    foreach ($customers as $customer) {
	$hosts = get_customer_hosts_sql($customer);
	$html .= "<h3>$customer</h3>\n<div>\n";
	$html .= "<button class=\"add_customer_btn\" data-id=\"$customer\">Edit customer</button><br/>\n";
	foreach ($hosts as $host) {
	    $html .= "<a href=\"?customer=$customer&hostname=$host\" id_cust=\"$customer\" host_name=\"$host\">Open host $host</a><br/> \n";
	}
	$html .= "</div>\n";
    }
    return $html."</div>\n";
}

function get_small_head () {
    return '<!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Statistics graphs</title>
	<link rel="stylesheet" href="css/jquery-ui-1.10.0.css" /> 
	<link rel="stylesheet" href="css/style.css">
	<link rel="stylesheet" href="css/start_menu.css">

	<script src="js/web/jquery-1.9.0.js"></script>
	<script src="js/web/jquery-ui-1.10.0.js"></script>
	<script src="js/me_first_page.js"></script>

    </head>
    <body>';
}

function get_head () {
    return '<!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Statistics graphs</title>

	<!--  ####################### CSS scripts #######################
	<!-- Bootstrap CSS Toolkit styles -->
	<!-- <link rel="stylesheet" href="http://blueimp.github.com/cdn/css/bootstrap.css"> -->
	<link rel="stylesheet" href="css/bootstrap.css">
	<!-- CSS to style the file input field as button and adjust the Bootstrap progress bars -->
	<!-- <link rel="stylesheet" href="https://raw.github.com/blueimp/jQuery-File-Upload/master/css/jquery.fileupload-ui.css"> -->
	<link rel="stylesheet" href="css/jquery.fileupload-ui.css">
	<!-- http://jqueryui.com/download/ -->
	<link rel="stylesheet" href="css/jquery-ui-1.10.0.css" /> 

	<!-- Generic page styles last, to let bootstrap do what it wants and overwrite what we want -->
	<link rel="stylesheet" href="css/style.css">


	<!--  ####################### Java scripts ####################### -->
	<!-- http://jquery.com/download/ -->
	<script src="js/web/jquery-1.9.0.js"></script>
	<script src="js/web/jquery-ui-1.10.0.js"></script>
	<!-- The Templates plugin is included to render the upload/download listings -->
	<!-- <script src="http://blueimp.github.com/JavaScript-Templates/tmpl.js"></script> -->
	<script src="js/web/tmpl.js"></script>
	
	<script src="js/jquery.fileupload.js"></script>		<!-- The basic File Upload plugin -->
	<script src="js/jquery.fileupload-fp.js"></script>	<!-- The File Upload file processing plugin -->
	<script src="js/jquery.fileupload-ui.js"></script>	<!-- The File Upload user interface plugin -->
	<script src="js/locale.js"></script>			<!-- The localization script -->
	<script src="js/main.js"></script>			<!-- The main application script -->

	<script src="js/me_update_elements.js"></script>
        <script src="js/progress_bar.js"></script>

    </head>
    <body>';
}

function get_footer() {
    return '



    </body>
</html>';
}

?> 
