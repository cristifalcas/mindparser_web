<?php

$db_link;
$db_user = 'mind_statistics';
$db_pass = '!0mind_statistics@9';
$db_database = 'mind_statistics';
$customers_table = 'customers';
$hosts_table = 'hosts';
$stats_file_table = 'statistics_files';
$customer="";
$host="";

session_start();

function connect_db() {
    global $db_link, $db_user, $db_pass, $db_database;
    $db_link = mysql_connect('localhost', $db_user, $db_pass);
    if (! ($db_link && mysql_select_db($db_database, $db_link))) {
	print 'Could not connect: ' . mysql_error();
        exit;
    }
}

function db_insert_file($customer, $host, $file_name, $processed_status) {
    global $stats_file_table;
    $create_table = "CREATE TABLE IF NOT EXISTS $stats_file_table
	( id int not null AUTO_INCREMENT,
	timestamp TIMESTAMP not null,
	customer varchar(50) not null,
	host varchar(50) not null,
	file_name varchar(2500) not null,
	processed int not null,
	PRIMARY KEY(id))";
    mysql_query($create_table) or die(mysql_error());

    $query = "INSERT IGNORE INTO $stats_file_table (timestamp, customer, host, file_name, processed) VALUES(".time()."'$customer', '$host', '$file_name', '$processed_status')";
    $result = mysql_query($query) or die("Error in query $query." .mysql_error()); 
    return $result;
}

function get_customers() {
    global $customers_table;
    $create_cust_table = "CREATE TABLE IF NOT EXISTS $customers_table
	( id int not null AUTO_INCREMENT,
	name varchar(50) UNIQUE,
	PRIMARY KEY(id))";
    mysql_query($create_cust_table) or die(mysql_error());

    $rs = mysql_query("select * from $customers_table;") or print "get_customers: ".mysql_error();
    while($row = mysql_fetch_array($rs)){
	$customers[$row['id']] = $row['name'];
    }
    return $customers;
    
}

function get_hosts($customer) {
    global $hosts_table;
    $machines = array();
    $create_cust_table = "CREATE TABLE IF NOT EXISTS $hosts_table
	( id int not null AUTO_INCREMENT,
	customer_id int,
	name varchar(50),
	ip varchar(50),
	unique index(customer_id, name),
	PRIMARY KEY(id),
	FOREIGN KEY (customer_id) REFERENCES customers(id) )";
    mysql_query($create_cust_table) or die(mysql_error());

    $rs = mysql_query("select * from $hosts_table where customer_id in (select id from customers where name='$customer');") or print "get_hosts: ".mysql_error();
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
    $all_customers = get_customers();
    foreach ($all_customers as $cust) {
	$all_hosts = get_hosts($cust);

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
<script type="text/javascript">
	var menu=new menu.dd("menu");
	menu.init("menu","menuhover");
</script>
<br/><br/><br/><br/>';
    return $html;
}

function get_header () {
    return '<!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Statistics graphs</title>
	<script type="text/javascript" src="js/script_menu.js"></script>
	<!-- Bootstrap CSS Toolkit styles -->
	<!-- <link rel="stylesheet" href="http://blueimp.github.com/cdn/css/bootstrap.min.css"> -->
	<link rel="stylesheet" href="css/bootstrap.css">
	<!-- Bootstrap styles for responsive website layout, supporting different screen sizes -->
	<!-- <link rel="stylesheet" href="http://blueimp.github.com/cdn/css/bootstrap-responsive.min.css"> -->
	<link rel="stylesheet" href="css/bootstrap-responsive.css">
	<!-- Bootstrap CSS fixes for IE6 -->
	<!--[if lt IE 7]><link rel="stylesheet" href="http://blueimp.github.com/cdn/css/bootstrap-ie6.min.css"><![endif]-->
	<!-- Bootstrap Image Gallery styles -->
	<!-- <link rel="stylesheet" href="http://blueimp.github.com/Bootstrap-Image-Gallery/css/bootstrap-image-gallery.min.css"> -->
	<link rel="stylesheet" href="css/bootstrap-image-gallery.css">
	<!-- CSS to style the file input field as button and adjust the Bootstrap progress bars -->
	<link rel="stylesheet" href="css/jquery.fileupload-ui.css">
	<!-- Shim to make HTML5 elements usable in older Internet Explorer versions -->
	<!--[if lt IE 9]><script src="http://html5shim.googlecode.com/svn/trunk/html5.js"></script><![endif]-->
	<!-- Generic page styles -->
	<link rel="stylesheet" href="css/style.css">
    </head>
    <body>';
}

function get_footer() {
    return '
	<!-- <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js"></script> -->
	<script src="js/web/jquery-1.7.2.js"></script>
	<!-- The jQuery UI widget factory, can be omitted if jQuery UI is already included -->
	<script src="js/vendor/jquery.ui.widget.js"></script>
	<!-- The Templates plugin is included to render the upload/download listings -->
	<!-- <script src="http://blueimp.github.com/JavaScript-Templates/tmpl.min.js"></script> -->
	<script src="js/web/tmpl.js"></script>
	<!-- The Load Image plugin is included for the preview images and image resizing functionality -->
	<!-- <script src="http://blueimp.github.com/JavaScript-Load-Image/load-image.min.js"></script> -->
	<script src="js/web/load-image.js"></script>
	<!-- The Canvas to Blob plugin is included for image resizing functionality -->
	<!-- <script src="http://blueimp.github.com/JavaScript-Canvas-to-Blob/canvas-to-blob.min.js"></script> -->
	<script src="js/web/canvas-to-blob.js"></script>
	<!-- Bootstrap JS and Bootstrap Image Gallery are not required, but included for the demo -->
	<!-- <script src="http://blueimp.github.com/cdn/js/bootstrap.min.js"></script> -->
	<!-- <script src="http://blueimp.github.com/Bootstrap-Image-Gallery/js/bootstrap-image-gallery.min.js"></script> -->
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
