<?php
//   print_r($_GET);
  // inotifywait -m -r --format '%w%f' -e close_write /var/www/html/coco/scripts/uploads/
  // error_reporting(E_STRICT);
  //  phpinfo();

  include_once('scripts/config.inc.php');
  connect();

  if(isset($_GET['customer']) && isset($_GET['hostname']) && validate_customer_host_sql($_GET['customer'], $_GET['hostname'])){
    echo get_head();
    $customer = $_GET['customer'];
    $hostname = $_GET['hostname'];
echo generateMenuInTable();
    echo generateUpload($customer, $hostname);
    echo generateStatusbar();
    echo get_footer();
  } else {
    echo get_small_head();
    echo generateAddCustomer();
    echo generate_customers();
    echo get_footer();
  }

  disconnect();
?>
