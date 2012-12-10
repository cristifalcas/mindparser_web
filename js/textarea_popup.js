var send_result_to_page = "qq.php";

function add_to_div (plugin_id, plugin_name, plugin_text) {
    var myForm = document.createElement("form");
    myForm.setAttribute('method',"post");
    myForm.setAttribute('action',send_result_to_page);
    myForm.setAttribute('id',"target");
    myForm.setAttribute('class',"plugin_id_"+plugin_id);

    var dialogDiv = document.createElement("div");
    dialogDiv.setAttribute('id',"dialog-form");
    dialogDiv.setAttribute('title',"Edit some stats");
    dialogDiv.setAttribute('class',"test1_"+plugin_id);

    var myTextare = document.createElement("textarea");
    myTextare.setAttribute('name',"name");
    myTextare.setAttribute('id',"textarea_edit_plugin");
    myTextare.setAttribute('class',"text ui-widget-content ui-corner-all");
    myTextare.innerHTML = plugin_text;

    var myA = document.createElement("a");
    myA.setAttribute('href',"#");
    myA.setAttribute('class',"test2_"+plugin_id);
    myA.innerHTML = "Edit plugin name "+plugin_name+plugin_id;

//     var holdAllDiv = document.createElement("div");
//     holdAllDiv.setAttribute('id',"hold_plugin_id_"+plugin_id);


    // we have parent_div->form->div->texarea and parent_div->a
    dialogDiv.appendChild(myTextare);
    myForm.appendChild(dialogDiv);
//     holdAllDiv.appendChild(myForm);
//     holdAllDiv.appendChild(myA);

    return [myForm, myA];
}

function runFunction() {
// $( "#dialog:ui-dialog" ).dialog( "destroy" );
//     allFields = $( [] ).add( name );



    
  var options = {
	autoOpen: false,
	height: 500,
	width: 550,
        modal: true,
        draggable: true,
	closeOnEscape: true,
	buttons: {
	    Submit: function() {
		    var name = $( "textarea."+$crt_textarea);
		    $.post(send_result_to_page ,{post: "name="+name.val()+$crt_textarea},function() {});
		    $( this ).dialog( "close" );
	    },
	    Cancel: function() {
		$( this ).dialog( "close" );
	    }
	},
	close: function() {
	    $( this ).dialog( "close" );
// 	    allFields.val( "" ).removeClass( "ui-state-error" );
	}
    };

//     var total=3;
// myArray = new Array(1, 2, 3);
    var pluginsDiv = document.getElementById("change_with_plugins");
    pluginsDiv.innerHTML = "";
//     for (var i=1;i<=total;i++){
  

var randomnumber=Math.floor(Math.random()*11);

var myObjs = document.getElementsByTagName("div"); // get element by tag name
for (var i = 0; i < myObjs.length; i++) {
// aria-labelledby="ui-id-1"
if (myObjs[i].hasAttribute("aria-labelledby")){
  qwe=1;
}
  if (myObjs[i].hasAttribute("aria-labelledby") && myObjs[i].getAttribute('aria-labelledby')!="ui-id-4" ){
    myObjs[i].parentNode.removeChild(myObjs[i]);
  }
}
 if (qwe){return};
	$.each([000, 100, 200, 300], function(i, value) {

	var result = add_to_div(i, "coco", "here comes jhonny "+i);
	pluginsDiv.appendChild(result[0]);
	pluginsDiv.appendChild(result[1]);

	    var dlg = $('.test1_'+i).dialog(options);
// 	    if (!dlg.dialog("isOpen")) { 
		$('.test2_'+i).click(function() {
		    $("textarea#textarea_edit_plugin").val('replacement string '+i);
		    $crt_textarea = "name"+i;
		    $old_var = $("textarea.name"+i).val();
		    dlg.dialog("open");
		});
// 	    }
	});
//     };
}

$(function() {
//   var t=setInterval(runFunction,3000);
// runFunction();
});

