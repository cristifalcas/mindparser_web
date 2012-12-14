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
$customer="";
$host="";

session_start();

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
    $query = "select * from $customers_table where id>0";
    $rs = mysql_query($query) or error_log("get_customers: $query || ".mysql_error());
    while($row = mysql_fetch_array($rs)){
	$customers[$row['id']] = $row['name'];
    }
    return $customers;
    
}

function get_hosts_sql($customer) {
    global $hosts_table, $customers_table;
    $machines = array();
    $query = "select h.* from $hosts_table h, $customers_table c where h.customer_id=c.id and c.name='$customer'";
    $rs = mysql_query($query) or error_log("get_hosts: $query || ".mysql_error());
    while($row = mysql_fetch_array($rs)){
	$machines[$row['id']] = $row['name'];
    }
    return $machines;
}

function validate($customer_name, $host_name) {
    global $hosts_table, $customers_table;
    $query = "select count(*) as nr from $hosts_table h, $customers_table c where h.customer_id=c.id and c.name='$customer_name' and h.name='$host_name'";
    $rs = mysql_query($query) or error_log("validate: $query || ".mysql_error());
    $row = mysql_fetch_array($rs);
    return $row['nr']==1;
}

function get_host_id ($customer_name, $host_name) {
    global $hosts_table, $customers_table;
    $query = "select h.id from $hosts_table h, $customers_table c where h.customer_id=c.id and c.name='$customer_name' and h.name='$host_name'";
    $rs = mysql_query($query) or error_log("validate: $query || ".mysql_error());
    $row = mysql_fetch_array($rs);
    return $row['id'];
}

function get_plugins_array($host_id) {
    global $plugins_table;
    $plugins = array();
    $query = "select * from $plugins_table where host_id='$host_id'";
    $rs = mysql_query($query) or error_log("get_plugins_array: $query || ".mysql_error());
    while($row = mysql_fetch_array($rs)){
	array_push($plugins, ["id"=>$row['id'], "name"=>$row['plugin_name']]);
    }
    return $plugins;
}

function get_plugin_def ($plugin_id){
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
// 	    $section = $value;
// 	    error_log("section $value =".trim("  sa   asf sad      sadasd sad sad   "));
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
    mysql_query($query) or error_log("set_plugin_def3: $query || ".mysql_error());
}

// function make_dirs() {
//     foreach (get_customers_sql() as $cust) {
// 	foreach (get_hosts_sql($cust) as $host) {
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

function generateSelect($name = '', $array = array(), $crt_selection, $input_array = array()) {
    $other_name = "";
    $other_value = "";
    if (sizeof($input_array) == 2){
	$other_name = $input_array['name'];
	$other_value = $input_array['value'];
    }
    $html = "\n<form method=\"POST\">
<input type=\"hidden\" name=\"$other_name\" value=\"$other_value\">
    <select id=\"customers\" name=\"".$name."\" onchange=\"this.form.submit();\">
      <option value=\"nada\">==Choose existing==</option>";
    foreach ($array as $id => $value) {
	$selected="";
	if (!is_null($crt_selection) && $value == $crt_selection) {$selected=" selected=\"selected\" ";}
	$html .= "<option id=$id value=".$value."$selected>".$value."</option>";
    }

    $html .= "</select></form>";
    return $html;
}

function generateUpload($array) {
    $selection = null;
    if (array_key_exists('menu_selection', $array)) {
      $selection = explode(",",$array['menu_selection']);
    };

    $body="";

    if (sizeof($selection) == 2){
	$customer = $selection[0];
	$host = $selection[1];
	$url="../cgi-bin/munin-cgi-html/$customer/index.html";

	$file_handle = fopen("scripts/load_body.html", "r");
	while (!feof($file_handle)) {
	  $body .= fgets($file_handle);
	}
	fclose($file_handle);
	$body = str_replace(array('$$customer$$','$$host$$', '$$url$$'), array($customer,$host, $url), $body);
// 	echo $body;
    }
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

function generateMenuInTable() {
    $html = '<table cellspacing="0" cellpadding="0" border="0" width="100%">
  <tr>
    <td width=160  valign=center>'. generateMenu().'</td>
    <td >
	<div style="display: none;" id="edit_plugins_forms_placer"></div>
	<div style="width:100%; height:60px; overflow:auto;" valign=top title="Edit plugin">
	    <a href="/get_some_help_here" ><img src="img/blue-circle-help-button.png" class="help_image"/></a>
	    <ul id="change_with_links_plugins"></ul>
	</div>
    </td>
  </tr>
</table><br/>
<div id="errors">Errors</div>
';
    return $html;
}

function generateMenu() {
    $html = '<form name="form_menu" method="get" action="index.php"> 
<input type="hidden" name="menu_selection" value="sfg"/>
<ul class="menu" id="menu">
	<li><a class="menulink">Select Customer</a>
		<ul>';
    $all_customers = get_customers_sql();
    foreach ($all_customers as $cust) {
	$all_hosts = get_hosts_sql($cust);

	if( !sizeof($all_hosts)){ continue;};
	$html .= "
			<li><a class=\"sub\">$cust</a>
				<ul>";
	$first = ' class="topline"';
	foreach ($all_hosts as $host) {
	    $html .= "
				    <li$first><a href=\"#\" onclick=\"document.form_menu.menu_selection.value='$cust,$host';document.form_menu.submit();\" >$host</a></li>";
	    $first = "";
	}
	$html .= "
				</ul>
			</li>";
    }
    $html .= '
		</ul>
	</li>
</ul> 
</form>
<script type="text/javascript">	var menu=new menu.dd("menu");menu.init("menu","menuhover");</script>';
// <br/><br/><br/>
    return $html;
}

function get_head () {
    return '<!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Statistics graphs</title>

	<!--  ####################### CSS scripts ####################### -->
	<link rel="stylesheet" href="css/dropdown_menu.css">

	<!-- Bootstrap CSS Toolkit styles -->
	<!-- <link rel="stylesheet" href="http://blueimp.github.com/cdn/css/bootstrap.min.css"> -->
	<link rel="stylesheet" href="css/bootstrap.css">
	<!-- CSS to style the file input field as button and adjust the Bootstrap progress bars -->
	<!-- <link rel="stylesheet" href="https://raw.github.com/blueimp/jQuery-File-Upload/master/css/jquery.fileupload-ui.css"> -->
	<link rel="stylesheet" href="css/jquery.fileupload-ui.css">
	<link rel="stylesheet" href="css/jquery-ui-1.9.2.css" /> 
	<!-- Shim to make HTML5 elements usable in older Internet Explorer versions -->
	<!--[if lt IE 9]><script src="http://html5shim.googlecode.com/svn/trunk/html5.js"></script><![endif]-->

	<!-- Generic page styles last, to let bootstrap do what it wants and overwrite what we want -->
	<link rel="stylesheet" href="css/style.css">


	<!--  ####################### Java scripts ####################### -->
	<!-- <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js"></script> -->
	<script src="js/web/jquery-1.8.3.js"></script>
	<script src="js/web/jquery-ui-1.9.2.js"></script> 

	<script type="text/javascript" src="js/dropdown_menu.js"></script>
	<script src="js/me_update_elements.js"></script>
        <!--  <script type="text/javascript" src="js/progress_bar.js"></script> -->

	<!-- The Templates plugin is included to render the upload/download listings -->
	<!-- <script src="http://blueimp.github.com/JavaScript-Templates/tmpl.min.js"></script> -->
	<script src="js/web/tmpl.js"></script>

	<!-- The Iframe Transport is required for browsers without support for XHR file uploads -->
	<script src="js/jquery.iframe-transport.js"></script>
	<!-- The basic File Upload plugin -->
	<script src="js/jquery.fileupload.js"></script>
	<!-- The File Upload file processing plugin -->
	<script src="js/jquery.fileupload-fp.js"></script>
	<!-- The File Upload user interface plugin -->
	<script src="js/jquery.fileupload-ui.js"></script>
	<!-- The localization script -->
	<script src="js/locale.js"></script>
	<!-- The main application script -->
	<script src="js/main.js"></script>
	<!-- The XDomainRequest Transport is included for cross-domain file deletion for IE8+ -->
	<!--[if gte IE 8]><script src="js/cors/jquery.xdr-transport.js"></script><![endif]-->

    </head>
    <body>';
}

function get_footer() {
    return '
    </body>
</html>';
}

?> 