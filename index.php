<?php
// inotifywait -m -r --format '%w%f' -e close_write /var/www/html/coco/scripts/uploads/
include_once('scripts/config.inc.php');
//     $_SESSION['upload_dir'] = "";
//     $_SESSION['customer'] = "";
//     $_SESSION['host'] = "";
connect_db();

if (isset($_GET['PROGRESS'])) {
  header('Pragma: no-cache');
  header('Cache-Control: no-cache, must-revalidate');
  header('Expires: Mon, 26 Jul 1997 05:00:00 GMT');
  header('Content-type: text/xml');
  print '<?xml version="1.0"?>';
  $progress = (time() % 50) * 2;

  print '<DOCUMENT><PROGRESS>';
  print (time() % 50) * 2;
  print '</PROGRESS></DOCUMENT>';
  mysql_close($db_link);
  return;
}

echo get_header();
print_r($_POST);
foreach (get_customers() as $cust) {
    foreach (get_hosts($cust) as $host) {
	## upload script is in another directory
	$dir_full_path = dirname($_SERVER['SCRIPT_FILENAME'])."/scripts/uploads/$cust/$host/";
	if (! is_dir ($dir_full_path)) {
	    umask(0);
	    mkdir($dir_full_path, 0775, true);
	    chgrp($dir_full_path, "nobody");
// 	    copy("scripts/uploads.htaccess", "$dir_full_path/.htaccess");
	}
    }
}

echo '<div class="wrapper">';
echo generateMenu();
$selection = null;
if (array_key_exists('menu_selection', $_POST)) {
  $selection = explode(",",$_POST['menu_selection']);
};

$body="";

if (sizeof($selection) == 2){
    $customer = $selection[0];
    $host = $selection[1];
    $url="../cgi-bin/munin-cgi-html/$customer/index.html";
//     $dir = "/uploads/$customer/$host/";
//     $_SESSION['upload_dir'] = $dir;
//     $_SESSION['customer'] = $customer;
//     $_SESSION['host'] = $host;

    $file_handle = fopen("scripts/load_body.html", "r");
    while (!feof($file_handle)) {
      $body .= fgets($file_handle);
    }
    fclose($file_handle);
    $body = str_replace(array('$$customer$$','$$host$$', '$$url$$'), array($customer,$host, $url), $body);
    echo $body;
}


mysql_close($db_link);

echo '    <div class="push"></div></div>
<div class="footer"><div id="progress_container"><div id="progress" style="width: 0%"></div></div></div>';

echo get_footer();
?>
