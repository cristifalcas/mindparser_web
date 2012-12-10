<!doctype html>
 
<html lang="en">
<!-- http://jqueryui.com/dialog/#modal-form -->
<head>
    <meta charset="utf-8" />
    <title>jQuery UI Dialog - Modal form</title>
	<link rel="stylesheet" href="../css/edit_popup.css">
	<link rel="stylesheet" href="../css/jquery-ui-1.9.2.css" /> 
	<script src="../js/web/jquery-1.8.3.js"></script>
	<script src="../js/web/jquery-ui-1.9.2.js"></script> 
	<script src="../js/textarea_popup.js"></script>
</head>
  <body>
    <form method="post" action="qq.php" id="target" >
      <div id="dialog-form" title="Edit some stats">
	<textarea name="name" id="name" class="text ui-widget-content ui-corner-all" >test</textarea>
      </div>
    </form>
    <button id="create-user">Some stats</button>
  </body>
</html>
