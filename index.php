<?php
//             limitConcurrentUploads: 5,
//             autoUpload: true,
//             maxFileSize: undefined,

// print_r($_POST);
// inotifywait -m -r --format '%w%f' -e close_write /var/www/html/coco/scripts/uploads/
// make dirs
include_once('scripts/config.inc.php');
connect_db();

//start page
echo get_head();

// menu
echo generateMenuInTable();

// echo generatePluginEditButton("1");
// echo generatePluginEditButton("2");
// echo generatePluginEditButton("111");
// echo generatePluginEditButton("1111");

// upload window
echo generateUpload($_POST);

close_db();

// progress bar at bottom
// echo '<div id="progress_container"><div id="progress_bar" style="width: 0%"></div></div>';

//endpage
echo get_footer();
?>
