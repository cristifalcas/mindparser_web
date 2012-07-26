<?php

include_once('scripts/config.inc.php');
$customer="";
$host = "";

// print_r($_POST);
connect_db();
echo '<!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <title>Edit customers</title>
	<link rel="stylesheet" href="css/style.css">
    </head>
    <body>';

if ($_POST){
    if (isset($_POST['cust'])) {
	$customer = $_POST['cust'];
    }
    if  (isset($_POST['host'])) {
	$host = $_POST['host'];
    }
    if (isset($_POST['del_cust'])) {
	$customer = $_POST['del_cust'];
	$all_hosts = get_hosts($customer);
	if (!sizeof($all_hosts)) {
	    $query = "delete from $customers_table where name='$customer'";
	    $result = mysql_query($query) or die("Error in query $query." .mysql_error()); 
	    $customer="";
	} else {
	    echo "Can't delete. Customer $customer still has hosts defined.";
	}
    }
    if (isset($_POST['del_host'])) {
	$host = $_POST['del_host'];
	$query = "delete from $hosts_table where name='$host' and customer_id=(select id from $customers_table where name='$customer')";
	$result = mysql_query($query) or die("Error in query $query." .mysql_error()); 
	$host = "";
    }
}

$all_customers = get_customers();

echo "<div class=\"me_styled-input\">\n<form name=\"input\" action=\"edit.php\" method=\"post\">
Add a new customer: <input type=\"text\" name=\"cust\" />
<input type=\"submit\" value=\"Submit\" />
</form>\n</div>\n";

if ( $customer != "") {
    $query = "INSERT IGNORE INTO $customers_table (name) VALUES('$customer')";
    $result = mysql_query($query) or die("Error in query $query." .mysql_error()); 
    $all_customers = get_customers();
}

echo "<p>Currently editing customer $customer.</p>";
echo "<div class=\"me_styled-select_edit\">\n".generateSelect('cust', $all_customers, $customer)."</div>\n";

if ( $customer != "") {
    ## delete
    echo "<form name=\"input\" action=\"edit.php\" method=\"post\">
    <input type=\"hidden\" name=\"del_cust\" value=$customer />
    <input type=\"submit\" value=\"Delete\" />
    </form>";

    $cust_id=array_keys($all_customers, $customer)[0];

    if ( $host != "") {
	$query = "INSERT IGNORE INTO $hosts_table (customer_id, name) VALUES('$cust_id','$host')";
	$result = mysql_query($query) or die("Error in query $query." .mysql_error()); 
    }
    echo "<br/><br/>Existing machines:";
    $all_hosts = get_hosts($customer);

    echo "<div class=\"me_styled-select_edit\">\n".generateSelect('host', $all_hosts, $host, array("name" => 'cust', "value" => $customer))."</div>\n";
    if ( $host != "") {
	echo "<form name=\"input\" action=\"edit.php\" method=\"post\">
	<input type=\"hidden\" name=\"del_host\" value=$host />
	<input type=\"hidden\" name=\"cust\" value=$customer />
	<input type=\"submit\" value=\"Delete\" />
	</form>";
    }

    echo "<div class=\"me_styled-input\">\n<form name=\"input\" action=\"edit.php\" method=\"post\">
    Add a new machine: <input type=\"text\" name=\"host\" />
    <input type=\"hidden\" name=\"cust\" value=$customer />
    <input type=\"submit\" value=\"Submit\" />
    </form>\n</div>\n";
}
echo '</body>
</html>';
mysql_close($db_link);
?>
