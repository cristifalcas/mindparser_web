<?php
// inotifywait -m -r --format '%w%f' -e close_write /var/www/html/coco/scripts/uploads/
include_once('scripts/config.inc.php');

echo get_header();
connect_db();

foreach (get_customers() as $cust) {
    foreach (get_hosts($cust) as $host) {
	## upload script is in another directory
	$dir_full_path = dirname($_SERVER['SCRIPT_FILENAME'])."/scripts/uploads/$cust/$host/";
	if (! is_dir ($dir_full_path)) {
	    mkdir($dir_full_path, 0775, true);
	    chgrp($dir_full_path, "nobody");
	    copy("scripts/uploads.htaccess", "$dir_full_path/.htaccess");
	}
    }
}

echo generateMenu();
$selection = null;
if (array_key_exists('menu_selection', $_POST)) {
  $selection = explode(",",$_POST['menu_selection']);
};

$body="";

if (sizeof($selection) == 2){
    $customer = $selection[0];
    $host = $selection[1];
    $url="";
    $dir = "/uploads/$customer/$host/";
    $_SESSION['upload_dir'] = $dir;
    $_SESSION['customer'] = $customer;
    $_SESSION['host'] = $host;

    $file_handle = fopen("scripts/load_body.html", "r");
    while (!feof($file_handle)) {
      $body .= fgets($file_handle);
    }
    fclose($file_handle);
    $body = str_replace(array('$$customer$$','$$host$$', '$$url$$'), array($customer,$host, $url), $body);
    echo $body;
}

mysql_close($db_link);
echo get_footer();

?>
