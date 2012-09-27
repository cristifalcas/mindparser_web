<?php
//             limitConcurrentUploads: 5,
//             autoUpload: true,
//             maxFileSize: undefined,

// inotifywait -m -r --format '%w%f' -e close_write /var/www/html/coco/scripts/uploads/
include_once('scripts/config.inc.php');
connect_db();

if (isset($_GET['PROGRESS'])) {
  header('Pragma: no-cache');
  header('Cache-Control: no-cache, must-revalidate');
  header('Expires: Mon, 26 Jul 1997 05:00:00 GMT');
  header('Content-type: text/xml');
  print '<?xml version="1.0"?>';
//   $progress = (time() % 50) * 2;
  $q= '<DOCUMENT><PROGRESS>'.((time() % 50) * 2).'</PROGRESS></DOCUMENT>';
  print $q;
// error_log("sd $q");
//   print ;
  close_db();
  return;
}

echo get_header();
print_r($_POST);
foreach (get_customers_sql() as $cust) {
    foreach (get_hosts_sql($cust) as $host) {
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

    $file_handle = fopen("scripts/load_body.html", "r");
    while (!feof($file_handle)) {
      $body .= fgets($file_handle);
    }
    fclose($file_handle);
    $body = str_replace(array('$$customer$$','$$host$$', '$$url$$'), array($customer,$host, $url), $body);
    echo $body;
}

close_db();

echo '   
<div id="progress_container">
  <div id="progress_bar" style="width: 0%"></div>
</div>';

echo get_footer();
?>
