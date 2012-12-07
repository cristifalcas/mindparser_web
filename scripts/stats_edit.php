<!doctype html>
 
<html lang="en">
<!-- http://jqueryui.com/dialog/#modal-form -->
<head>
    <meta charset="utf-8" />
    <title>jQuery UI Dialog - Modal form</title>
    <link rel="stylesheet" href="http://code.jquery.com/ui/1.9.2/themes/base/jquery-ui.css" />
    <script src="http://code.jquery.com/jquery-1.8.3.js"></script>
<!--     <script src="/resources/demos/external/jquery.bgiframe-2.1.2.js"></script> -->
    <script src="http://code.jquery.com/ui/1.9.2/jquery-ui.js"></script>
<!--     <link rel="stylesheet" href="/resources/demos/style.css" /> -->
    <style>
        body { font-size: 62.5%; }
textarea {
 margin-bottom:1px; 
/* padding: .4em; */
/* display:block; */
width:99%;
height:99%; 
      -webkit-box-sizing: border-box; /* Safari/Chrome, other WebKit */ 
     -moz-box-sizing: border-box;    /* Firefox, other Gecko */
     box-sizing: border-box;         /* Opera/IE 8+ */
}
    </style>
    <script>
    $(function() {
        var name = $( "#name" );



        $( "#dialog-form" ).dialog({
            autoOpen: false,
            height: 500,
            width: 550,
//             modal: true,
            buttons: {
                Submit: function() {
$.post("qq.php",{post: "name="+name.val()},
      function(data) {
//           $("textarea").val(data);
//           alert('done');
});
			$("textarea#name").val(name.val());
			$( this ).dialog( "close" );


                },
                Cancel: function() {
                    $( this ).dialog( "close" );
                }
            },
            close: function() {
                allFields.val( "" ).removeClass( "ui-state-error" );
            }
        });
$('textarea').blur(function() {
  if($.trim($('textarea').val()).length){
    // Added () around $this
    $(this).css('background-image', 'none');
  } else {
    $(this).css('background-image', $.data(this, 'img'));
  }
});
        $( "#create-user" )
            .button()
            .click(function() {
                $( "#dialog-form" ).dialog( "open" );
            });
    });
    </script>
</head>
<body>

<form method="post" action="qq.php" id="target" >
<div id="dialog-form" title="Edit some stats">
<!--     <fieldset> -->
        <textarea name="name" id="name" class="text ui-widget-content ui-corner-all" >def
fdsg
dfg
dgjh
hj</textarea>
<!--     </fieldset> -->
</div>
</form>
<button id="create-user">Some stats</button>

</body>
</html>