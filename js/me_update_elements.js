"use strict";

function sort_array(arr) {
    return arr.sort(function(a,b){return a - b});
}

function createPluginHTMLElements (plugin_id, plugin_name) {
    var form = '<form id="form_edit_plugin_'+plugin_id+'" method="post" action="scripts/get_info.php">\
	<div id="div_edit_plugin_'+plugin_id+'" title="Edit stats for '+plugin_name+'" class="div_edit_plugin">\
	    <textarea id="textarea_edit_plugin_'+plugin_id+'" class="css_textarea_edit_plugin"></textarea>\
	    <p>\
		<label class="select_rate">Sample rate:</label>\
		<input class="select_rate" id="input_edit_plugin_'+plugin_id+'" value="-1"/>\
		<label class="select_rate" style="color:red;">*(Updating this value will rebuild the graphs for the entire plugin)</label>\
	    </p>\
	</div>\
    </form>'

    var link='<li id="li_spaces_me">\
	<a id="link_plugin_'+plugin_id+'" class="link_plugin">'+plugin_name+'</a>\
    </li>'

    return [form, link];
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
      a[0].parentNode.parentNode.removeChild(a[0].parentNode);
  }
}

function get_plugin_text(response, textStatus, XMLHttpRequest) {
    if($.isEmptyObject(response)){
	  return;
    };
    var text = response.text;
    var plugin_id = response.id;
    var update_rate = response.update_rate;

    $('#textarea_edit_plugin_'+plugin_id)[0].readOnly = false;
    $('#textarea_edit_plugin_'+plugin_id).val(text);
    $('#input_edit_plugin_'+plugin_id).val(update_rate);
//     $('input.original_rate_').removeClass('original_rate_');
    $('#input_edit_plugin_'+plugin_id).addClass('original_rate_'+update_rate);
}

function set_plugin_text(response, textStatus, XMLHttpRequest) {
    // done updating plugin info remotely
    var pluginsDiv;
    pluginsDiv = document.getElementById("errors");
    pluginsDiv.innerHTML = "errors in set_plugin_text: "+textStatus;
}

function rebuild_plugin(response, textStatus, XMLHttpRequest) {
}

function add_plugin_ids(plugins, plugins_arr){
      var options = {
	autoOpen: false,
	height: 500,
	width: 550,
        modal: true,
        draggable: true,
	closeOnEscape: true,
	open: function(event, ui){
	  $('.ui-dialog-buttonpane').find('button:contains("Cancel")').button({icons: {primary: 'ui-icon-cancel'}});
	  $('.ui-dialog-buttonpane').find('button:contains("Submit")').button({icons: {primary: 'ui-icon-circle-check'}});
	  $('.ui-dialog-buttonpane').find('button:contains("Rebuild")').button({icons: {primary: 'ui-icon-alert'}});
	  $('.ui-dialog-buttonpane').find('button:contains("Delete")').button({icons: {primary: 'ui-icon-trash'}});

	  $('<a />', {
	    'class': 'link_help_me ',
	    text: "Help",
	    title: "Help",
	    href: '#'
	  })
	  .button({icons: {primary: "ui-icon-help"}})
	  .appendTo($('.ui-dialog-buttonpane'))
	  .click(function(){
		$(event.target).dialog('close');
	  });

// 	    $('<input />', {'id': 'select_rate', 'name':'value'}).appendTo($('.ui-dialog-buttonpane')); //.spinner()
	},
	buttons: {
	    Submit: function() {
		var dialog_id = parseFloat($(this).attr('id').replace('div_edit_plugin_',""));
		var classList = $('#input_edit_plugin_'+dialog_id).attr('class').split(/\s+/);
		var original_rate;
		$.each( classList, function(index, item){
		    if ( item.match(/^original_rate_\d+$/) ) {
		      original_rate = parseInt(item.replace(/\D/g, ''), 10);
		    }
		});
		var crt_rate = $('#input_edit_plugin_'+dialog_id).val();

		var tsok = true;
		if (crt_rate != original_rate) {
		    var alert_text = 'Sample rate has been modified('+crt_rate+' vs '+original_rate+')\nAre you sure?';
		    tsok = confirm(alert_text);
		}
		if (tsok) {
		    var text = $('textarea#textarea_edit_plugin_'+dialog_id);
		    var arr = get_customer_host_name();
		    var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"set_plugin_text", extra:{text:text.val(), id:dialog_id, sample_rate:crt_rate}} };
		    post_data_home(JSONstring, set_plugin_text);
		    $( this ).dialog( "close" );
		}
	    },
	    Cancel: function() {
		$( this ).dialog( "close" );
	    },
	    'Rebuild graphs': function() {
		if (confirm('Are you sure?\nThis may take a few hours to complete.')){
		    var dialog_id = parseFloat($(this).attr('id').replace('div_edit_plugin_',""));
		    var crt_rate = $('#input_edit_plugin_'+dialog_id).val();
		    var text = $('textarea#textarea_edit_plugin_'+dialog_id);
		    var arr = get_customer_host_name();
		    var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"rebuild_plugin", extra:{text:text.val(), id:dialog_id, sample_rate:crt_rate}} };
		    post_data_home(JSONstring, rebuild_plugin);
		    $( this ).dialog( "close" );
		}
	    },
	    'Delete plugin': function() {
	    }
	},
	close: function() {
	    $('a.link_help_me').remove() ;
	    $( this ).dialog( "close" );
// 	    allFields.val( "" ).removeClass( "ui-state-error" );
	}
      };

      $.each(plugins_arr, function(i, plugin_id) {
	  if(typeof plugins[i] === 'undefined'){
	      alert ("Err: "+i);
      }
      var plugin_name = plugins[i].name;
      var result = createPluginHTMLElements(plugin_id, plugin_name);
      $('#edit_plugins_forms_placer').append(result[0]);
      $('#change_with_links_plugins').append(result[1]);

      var dlg = $('#div_edit_plugin_'+plugin_id).dialog(options);
      $('#link_plugin_'+plugin_id).click(function() {
	      var arr = get_customer_host_name();
	      var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"get_plugin_text", extra:{id:plugin_id}} };
	      post_data_home(JSONstring, get_plugin_text);
  // 	    $('.textarea_edit_plugin_'+plugin_id)[0].innerHTML = "";
	      $('#textarea_edit_plugin_'+plugin_id)[0].readOnly = true;
	      dlg.dialog("open");
	      return false;
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
	existing_plugin_ids[i] = parseFloat(existing_divs[i].id.replace('div_edit_plugin_',""));
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
    if(typeof arr === 'undefined'){return};
    var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"get_plugins", extra:{}} };
    post_data_home(JSONstring, updatePlugins);
}

function complete_cust() {
    $( "#autocomplete_customers" )
    // don't navigate away from the field on tab when selecting an item
    .bind( "keydown", function( event ) {
	if ( event.keyCode === $.ui.keyCode.TAB &&
	$( this ).data( "autocomplete" ).menu.active ) {
	event.preventDefault();
	}
    })
    .autocomplete({
	source: function( request, response ) {
	    var JSONstring = { customer:"", hostname:"", data:{function:"get_customers_autocomplete", extra:{request:request.term}} };
	    post_data_home(JSONstring, response);
	},
	focus: function() {return false;},
    });
}

function input_text() {
    $(".defaultText").focus(function(srcc)
    {
        if ($(this).val() == $(this)[0].title)
        {
            $(this).removeClass("defaultTextActive");
            $(this).val("");
        }
    });
    
    $(".defaultText").blur(function()
    {
        if ($(this).val() == "")
        {
            $(this).addClass("defaultTextActive");
            $(this).val($(this)[0].title);
        }
    });
    
    $(".defaultText").blur();
}

function switch_select() {
    $(".switch_selector").click(function(){
	var hiddenEls  = $("div.selector").filter(":hidden");
	var visibleEls = $("div.selector").filter(":visible");
	$.each( hiddenEls, function(index, item){
	  $(item).show();
		});
	$.each( visibleEls, function(index, item){
	  $(item).hide();
		});
	return false;
    });
}

function init() {
    complete_cust();
    input_text();
    switch_select();
}


$(function() {
  init()
  updates();
  var t1=setInterval('updates()', 1000);
});
