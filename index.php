<?php
//             limitConcurrentUploads: 5,
//             autoUpload: true,
//             maxFileSize: undefined,

// print_r($_POST);
// inotifywait -m -r --format '%w%f' -e close_write /var/www/html/coco/scripts/uploads/
// error_reporting(E_STRICT);

include_once('scripts/config.inc.php');
connect_db();

//start page
echo get_head();

// menu
echo generateMenuInTable();

// upload window
echo generateUpload($_GET);

close_db();

// progress bar at bottom
echo generateStatusbar();

//endpage
echo get_footer();
?>
