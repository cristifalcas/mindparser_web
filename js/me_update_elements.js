"use strict";

function sort_array(arr) {
    return arr.sort(function(a,b){return a - b});
}

function createPluginHTMLElements (plugin_id, plugin_name) {
    var myForm = document.createElement("form");
    myForm.setAttribute('id',"form_edit_plugin_"+plugin_id);
    myForm.setAttribute('method',"post");
    myForm.setAttribute('action',"qq.php");

    var dialogDiv = document.createElement("div");
    dialogDiv.setAttribute('id',"div_edit_plugin_"+plugin_id);
    dialogDiv.setAttribute('title',"Edit stats for "+plugin_name);

    var myTextare = document.createElement("textarea");
    myTextare.setAttribute('id',"textarea_edit_plugin");
    myTextare.setAttribute('class',"textarea_edit_plugin_"+plugin_id);
//     myTextare.innerHTML = plugin_text;

    var myLI = document.createElement("li");
    myLI.setAttribute('id',"li_spaces_me");
    
    var myA = document.createElement("a");
    myA.setAttribute('id',"link_plugin_"+plugin_id);
    myA.setAttribute('class',"link_plugin");
    myA.innerHTML = ""+plugin_name;

    myLI.appendChild(myA);

    // we have parent_div->form->div->texarea and parent_div->a
    dialogDiv.appendChild(myTextare);
    myForm.appendChild(dialogDiv);

    return [myForm, myLI];
}

function clear_plugin_ids (ids_arr){
    for (var i = 0; i < ids_arr.length; i++) {
      if ( $("#div_edit_plugin_"+ids_arr[i]).closest('.ui-dialog').is(':visible') === true ) {
	  $("#div_edit_plugin_"+ids_arr[i]).dialog( "close" );
      }
      // delete parent of div_edit_plugin_i
      var div = $( "div#div_edit_plugin_"+ids_arr[i]);
      if (div.length != 1) {
	  alert ("Probleme gicule!!"+ids_arr[i]+" length="+div.length+" arr="+ids_arr);
      }
      div[0].parentNode.parentNode.removeChild(div[0].parentNode);

      // delete form_edit_plugin_i
      var form = $( "form#form_edit_plugin_"+ids_arr[i]);
      if (form.length != 1) {
	  alert ("Probleme gicutule!!");
      }
      form[0].parentNode.removeChild(form[0]);

      // delete parent of link_plugin_i
      var a = $( "a#link_plugin_"+ids_arr[i]);
      if (a.length != 1) {
	  alert ("Probleme :((!!"+ids_arr[i]+" length="+a.length+" arr="+ids_arr);
      }
//       a[0].parentNode.removeChild(a[0]);
      a[0].parentNode.parentNode.removeChild(a[0].parentNode);
  }
}

function get_plugin_text(response, textStatus, XMLHttpRequest) {
    if($.isEmptyObject(response)){
	  return;
    };
    var text = response.text;
    var plugin_id = response.id;
    $('.textarea_edit_plugin_'+plugin_id)[0].readOnly = false;
    $('.textarea_edit_plugin_'+plugin_id).val(text);
}

function set_plugin_text(response, textStatus, XMLHttpRequest) {
    // done updating plugin info remotely
    return;
}

function add_plugin_ids(plugins, plugins_arr){
      var options = {
	autoOpen: false,
	height: 500,
	width: 550,
        modal: true,
        draggable: true,
	closeOnEscape: true,
// 	open: function(event, ui){
// 	    $( textarea.textarea_edit_plugin).val("qweasdzxc");
// //             $('<a />', {
// //                 'class': 'linkClass',
// //                 text: 'Cancel',
// //                 href: '#'
// //             })
// //             .appendTo($(".ui-dialog-buttonpane"))
// //             .click(function(){
// //                  $(event.target).dialog('close');
// //             });
// 	},
	buttons: {
	    Submit: function() {
		    var name = $("textarea");
		var arr = get_customer_host_name();
		var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"set_plugin_text", extra:{text:name.val()}} };
		post_data_home(JSONstring, set_plugin_text, false);
	    
// 		    var JSONstring = { text:name.val() };
// 		    var dat = JSON.stringify(frm.serializeArray());
// 		    $.post("qq.php" ,{post: "name="+name.val()},function() {});
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

    var divPlacer = document.getElementById("edit_plugins_forms_placer");
    var linksPlacer = document.getElementById("change_with_links_plugins");

    $.each(plugins_arr, function(i, plugin_id) {
	if(typeof plugins[i] === 'undefined'){
	    alert ("Err: "+i);
	}
	var plugin_name = plugins[i].name;
	var result = createPluginHTMLElements(plugin_id, plugin_name);
	divPlacer.appendChild(result[0]);
	linksPlacer.appendChild(result[1]);
	var dlg = $('#div_edit_plugin_'+plugin_id).dialog(options);
	$('#link_plugin_'+plugin_id).click(function() {
	    var arr = get_customer_host_name();
	    var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"get_plugin_text", extra:{id:plugin_id}} };
	    post_data_home(JSONstring, get_plugin_text);
// 	    $('.textarea_edit_plugin_'+plugin_id)[0].innerHTML = "";
	    $('.textarea_edit_plugin_'+plugin_id)[0].readOnly = true;
	    dlg.dialog("open");
	});
	return;
    });
}

// function retrievePlugins(){
//     var myplugins=[[99,1, 2, 3, 5, 54],[99,1, 5,6,7,9],[99,1,3,6,7,8,9],[99,2,1,3,4,5,6],[99,1,2,4,6,7],[1,3,5,67,89],[1,34,6,8],[,2,5,7,9],[1,2,3,4,567],[1,5,8,11],[1,4,5,7,8,9]];
//     var randomnumber = Math.floor(Math.random()*11); //0-10
//     var crt_plugin_ids = myplugins[randomnumber];
//     if(typeof crt_plugin_ids === 'undefined'){
// 	return;
//     };
// 
//     return sort_array(crt_plugin_ids);
// }

function updatePlugins(response, textStatus, XMLHttpRequest) {
    Array.prototype.diff = function(a) {
	return this.filter(function(i) {return !(a.indexOf(i) > -1);});
    };

    var existing_divs = $("*[id^='div_edit_plugin_']")
    var existing_plugin_ids = new Array;
    for (var i = 0; i < existing_divs.length; i++) {
	existing_plugin_ids[i] = parseFloat(existing_divs[i].id.replace("div_edit_plugin_",""));
    }
    existing_plugin_ids = sort_array(existing_plugin_ids);

    if($.isEmptyObject(response)){
	clear_plugin_ids(existing_plugin_ids);
	return;
    };
    var plugins = response.plugins;
    var crt_plugin_ids = [];
    for (var i = 0; i < plugins.length; i++) {
	crt_plugin_ids[i] = parseFloat(plugins[i].id);
    }
    crt_plugin_ids = sort_array(crt_plugin_ids);

//     crt_plugin_ids = retrievePlugins();
    
//     var pluginsDiv;
//     pluginsDiv = document.getElementById("existing_plugins");
//     pluginsDiv.innerHTML = "existing_plugins crt: "+existing_plugin_ids;
// 
//     pluginsDiv = document.getElementById("test_diff");
//     pluginsDiv.innerHTML = "current: "+crt_plugin_ids;
// 
//     pluginsDiv = document.getElementById("new_plugins");
//     pluginsDiv.innerHTML = "new_plugins to add: "+crt_plugin_ids.diff(existing_plugin_ids);
// 
//     pluginsDiv = document.getElementById("removed_plugins");
//     pluginsDiv.innerHTML = "removed_plugins to remove: "+existing_plugin_ids.diff( crt_plugin_ids );
    
    clear_plugin_ids(existing_plugin_ids.diff( crt_plugin_ids ));
    add_plugin_ids(plugins, crt_plugin_ids.diff(existing_plugin_ids));
}

// function crtl_progress_bar(status){
//     var pluginsDiv = document.getElementById("test_diff");
// //     pluginsDiv.innerHTML = "testes diff not ready: ";
// 
//     if (status == 'ok') {
// 	// get progress from the XML node and set progress bar width and innerHTML
// 	var level;
// 	level = request.responseXML.getElementsByTagName('PROGRESS')[0].firstChild;
// // 	progress_bar.style.width = progress_bar.innerHTML = level.nodeValue + '%';
// 	pluginsDiv.innerHTML = "testes diff ok: "+level.nodeValue;
//     } else {
// // 	progress_bar.style.width = '100%';
// // 	progress_bar.innerHTML = 'Error:[' + request.status + ']' + request.statusText;
// 	pluginsDiv.innerHTML = "testes diff bad: ";
//     }
// }

function success_progress_bar(response, textStatus, XMLHttpRequest) {
    var clientid = response.percent;
    var pluginsDiv = document.getElementById("test_diff");
    pluginsDiv.innerHTML = "testes diff2 ok: "+clientid;
}

function post_data_home( JSONstring, ctrl_funct, async) {
    async = typeof async !== 'undefined' ? async : true;
    $.ajax({
	type: "POST",
	url: "scripts/get_info.php",
	aync:async,
	data: { json: JSON.stringify(JSONstring) },
	success: ctrl_funct,
	failure: function(errMsg) {alert(errMsg);}
    });
}

function get_customer_host_name() {
    var url = $('#fileupload').prop('action');
    if(typeof url === 'undefined'){
	return;
    };
    var arr = url.split('?')[1].split('&');
    var customer = arr[0].replace(/^customer=/, "");
    var hostname = arr[1].replace(/^host=/, "");
    return [customer, hostname];
}

function updates() {
    var arr = get_customer_host_name();

    var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"get_plugins", extra:{}} };
    post_data_home(JSONstring, updatePlugins);


//     var JSONstring = { customer:customer, hostname:hostname, request_type:"get", data:"progress_bar" };
//     post_data_home(JSONstring, success_progress_bar);
    
//     pluginsDiv.innerHTML = "testes diff3 ok: ";
// success_progress_bar();

/*
    $.ajax({
  type: 'POST',
  url: "scripts/get_info.php",
  data: {{customer:"Peter", hostname:"Peter", request_type:"Peter", data:"Peter"}},
  dataType: "xml",
  success: function(xml){
    var clientid = $(xml).find('PROGRESS').eq(1).text();
    alert(clientid);
  }   
});*/
//     var question = "customer="+customer+"&hostname="+hostname+"&request_type="+request_type+"&data="+data;
//     ctrl_funct = crtl_progress_bar;
//     send_request(JSONstring);
}

// function up

$(function() {
// request = initXMLHttpClient();
//     var progress;
//     progress = document.getElementById('progress');

    var t1=setInterval('updates()', 1000);
//     var t2=setInterval('progress_bar()',  1000);
//     setInterval(send_request('fname=Peter&age=37&PROGRESS=new', progress_bar), 1000); 
});


