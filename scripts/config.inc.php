<?php

$db_link;
$db_user = 'mind_statistics';
$db_pass = '!0mind_statistics@9';
$db_database = 'mind_statistics';
$customers_table = '__customers';
$hosts_table = '__hosts';
$customer="";
$host="";

session_start();

function connect_db() {
    global $db_link, $db_user, $db_pass, $db_database;
    $db_link = mysql_connect('localhost', $db_user, $db_pass);
    if (! ($db_link && mysql_select_db($db_database, $db_link))) {
	print 'Could not connect: ' . mysql_error()."</br>";
        exit;
    }
}

function close_db() {
  global $db_link;
  mysql_close($db_link);
}

function get_customers_sql() {
    global $customers_table;
    $rs = mysql_query("select * from $customers_table where id>0;") or print "get_customers: ".mysql_error()."</br>";
    while($row = mysql_fetch_array($rs)){
	$customers[$row['id']] = $row['name'];
    }
    return $customers;
    
}

function get_hosts_sql($customer) {
    global $hosts_table, $customers_table;
    $machines = array();
    $query = "select * from $hosts_table where customer_id in (select id from $customers_table where name='$customer')";
    $rs = mysql_query($query) or print "get_hosts: $query".mysql_error()."</br>";
    while($row = mysql_fetch_array($rs)){
	$machines[$row['id']] = $row['name'];
    }
    return $machines;
}

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

function generateMenu() {
    $html = '
<form name="form_menu" method="post" action="index.php"> 
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
<script type="text/javascript">	var menu=new menu.dd("menu");menu.init("menu","menuhover");</script>
<br/><br/><br/><br/>';
    return $html;
}

function get_header () {
    return '<!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Statistics graphs</title>

	<link rel="stylesheet" href="css/progress_bar.css">
        <script type="text/javascript" src="js/progress_bar.js"></script>
	<link rel="stylesheet" href="css/dropdown_menu.css">
	<script type="text/javascript" src="js/dropdown_menu.js"></script>

	<!-- Bootstrap CSS Toolkit styles -->
	<!-- <link rel="stylesheet" href="http://blueimp.github.com/cdn/css/bootstrap.min.css"> -->
	<link rel="stylesheet" href="css/bootstrap.css">
	<!-- CSS to style the file input field as button and adjust the Bootstrap progress bars -->
	<!-- <link rel="stylesheet" href="https://raw.github.com/blueimp/jQuery-File-Upload/master/css/jquery.fileupload-ui.css"> -->
	<link rel="stylesheet" href="css/jquery.fileupload-ui.css">

	<!-- Shim to make HTML5 elements usable in older Internet Explorer versions -->
	<!--[if lt IE 9]><script src="http://html5shim.googlecode.com/svn/trunk/html5.js"></script><![endif]-->

	<!-- Generic page styles last, to let bootstrap do what it wants and overwrite what we want -->
	<link rel="stylesheet" href="css/style.css">
    </head>
    <body>';
}

function get_footer() {
    return '

	<!-- <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js"></script> -->
	<script src="js/web/jquery-1.8.3.js"></script>
	<!-- The jQuery UI widget factory, can be omitted if jQuery UI is already included -->
	<script src="js/vendor/jquery.ui.widget.js"></script>
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
    </body>
</html>';
}


?> 