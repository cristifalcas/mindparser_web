<?php
//             limitConcurrentUploads: 5,
//             autoUpload: true,
//             maxFileSize: undefined,

// inotifywait -m -r --format '%w%f' -e close_write /var/www/html/coco/scripts/uploads/
print_r($_POST);

include_once('scripts/config.inc.php');
echo get_header();
connect_db();

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

// echo '   
// <div id="progress_container">
//   <div id="progress_bar" style="width: 0%"></div>
// </div>';

echo get_footer();
?>
