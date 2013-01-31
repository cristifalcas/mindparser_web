"use strict";

Array.prototype.diff = function(a) {
    return this.filter(function(i) {return !(a.indexOf(i) > -1);});
};

function sort_array(arr) {
    return arr.sort(function(a,b){return a - b});
}

function createPluginHTMLElements (plugin_id, plugin_name) {
//   console.log("a",plugin_id, "q", plugin_name,"e");
    var form = '<div id="div_edit_plugin_'+plugin_id+'" title="Edit stats for '+plugin_name+'">\
	    <textarea id="textarea_edit_plugin_'+plugin_id+'" class="css_textarea_edit_plugin"></textarea>\
	    <p>\
		<label class="select_rate">Sample rate:</label>\
		<input class="select_rate" id="input_edit_plugin_'+plugin_id+'" value="-1"/>\
		<label class="select_rate" style="color:red;">*(Updating this value will rebuild the graphs for the entire plugin)</label>\
	    </p>\
	</div>';

    var link_edit  ='<li><a id="link_edit_plugin_'+plugin_id+'"  class="link_plugin">'+plugin_name+'</a></li>'
    var link_graphs='<li><a id="link_graph_plugin_'+plugin_id+'" class="link_plugin" href="'+plugin_name+'">'+plugin_name+'</a></li>'

    return [form, link_edit, link_graphs];
}

function get_plugin_text_success(response, textStatus, XMLHttpRequest) {
    if($.isEmptyObject(response)){
	  return;
    };
    var text = response.text;
    var plugin_id = response.id;
    var update_rate = response.update_rate;

    $('#textarea_edit_plugin_'+plugin_id).val(text);
    $('#textarea_edit_plugin_'+plugin_id)[0].readOnly = false;
    $('#input_edit_plugin_'+plugin_id).val(update_rate);
//     $('input.original_rate_').removeClass('original_rate_');
    $('#input_edit_plugin_'+plugin_id).addClass('original_rate_'+update_rate);
}

function set_plugin_text_success(response, textStatus, XMLHttpRequest) {
    // done updating plugin info remotely
    write_error_div("done set plugin text");
}

function write_error_div(text) {
    $("#errors").text("errors in: "+text).show(0);
}

function rebuild_plugin_success(response, textStatus, XMLHttpRequest) {
    write_error_div("rebuild_plugin_success");
}

function delete_plugin_success(response, textStatus, XMLHttpRequest) {
    write_error_div("delete_plugin_success");
}

function submit_plugin(elem){
    var dialog_id = parseFloat(elem.attr('id').replace('div_edit_plugin_',""));
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
	post_data_home(JSONstring, set_plugin_text_success);
	elem.dialog( "close" );
    }
}

function rebuild_plugin_click(elem){
  if (confirm('Are you sure?\nThis may take a few hours to complete.')){
      var dialog_id = parseFloat(elem.attr('id').replace('div_edit_plugin_',""));
      var crt_rate = $('#input_edit_plugin_'+dialog_id).val();
      var text = $('textarea#textarea_edit_plugin_'+dialog_id);
      var arr = get_customer_host_name();
      var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"rebuild_plugin", extra:{text:text.val(), id:dialog_id, sample_rate:crt_rate}} };
      post_data_home(JSONstring, rebuild_plugin_success);
      elem.dialog( "close" );
  }
}

function delete_plugin() {
  if (confirm('Are you sure?')){
      var dialog_id = parseFloat(elem.attr('id').replace('div_edit_plugin_',""));
      var arr = get_customer_host_name();
      var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"delete_plugin", extra:{id:dialog_id}} };
      post_data_home(JSONstring, delete_plugin_success);
      elem.dialog( "close" );
  }
}

function add_plugin_ids(plugins, plugins_arr){
  var options = {
	autoOpen: false,
	height: 530,
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
	    href: '/some/help/here'
	  })
	  .button({icons: {secondary: "ui-icon-help"}, text: false})
	  .appendTo($('.ui-dialog-buttonpane'))
	  .click(function(){$(event.target).dialog('close');});
	},
	buttons: {
	    Submit: function() {
		submit_plugin($(this));
	    },
	    Cancel: function() {
		$( this ).dialog( "close" );
	    },
	    'Rebuild graphs': function() {
		  rebuild_plugin_click($(this));
	    },
	    'Delete plugin': function() {
		  delete_plugin($(this));
	    }
	},
	close: function() {
	    $('a.link_help_me').remove() ;
	}
      };

  $.each(plugins_arr, function(i, plugin_id) {
// 	  if(typeof plugins[i] === 'undefined'){
// 	      alert ("Err: "+i);
// 	  }
      var plugin_name = plugins[i].name;
      var result = createPluginHTMLElements(plugin_id, plugin_name);
      $('div#edit_plugins_forms_placer').append(result[0]);
      $('ul#change_edit_plugins').append(result[1]);
      $('ul#change_view_plugins').append(result[2]);
      
      var dlg = $('#div_edit_plugin_'+plugin_id).dialog(options);
      $('a#link_edit_plugin_'+plugin_id).click(function() {
	  var arr = get_customer_host_name();
	  var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"get_plugin_text", extra:{id:plugin_id}} };
	  post_data_home(JSONstring, get_plugin_text_success);
	  $('#textarea_edit_plugin_'+plugin_id)[0].readOnly = true;
	  dlg.dialog("open");
	  return false;
      });

      return;
  });
}

function clear_plugin_ids (ids_arr){
    for (var i = 0; i < ids_arr.length; i++) {
      // close if dialog is opened
      if ( $("#div_edit_plugin_"+ids_arr[i]).closest('.ui-dialog').is(':visible') === true ) {
	  $("#div_edit_plugin_"+ids_arr[i]).dialog( "close" );
      }
      // delete parent of div_edit_plugin_i
      var div = $( "div#div_edit_plugin_"+ids_arr[i]);
      if (div.length != 1) {
	  alert ("Probleme gicule!!"+ids_arr[i]+" length="+div.length+" arr="+ids_arr);
      }
      div[0].parentNode.parentNode.removeChild(div[0].parentNode);

      // delete parent of link_edit_plugin_i
      var a = $( "a#link_edit_plugin_"+ids_arr[i]);
      if (a.length != 1) {
	  alert ("Probleme :((!!"+ids_arr[i]+" length="+a.length+" arr="+ids_arr);
      }
      a[0].parentNode.parentNode.removeChild(a[0].parentNode);

      // delete parent of link_graph_plugin_i
      var b = $( "a#link_graph_plugin_"+ids_arr[i]);
      b[0].parentNode.parentNode.removeChild(a[0].parentNode);
  }
}

function updatePlugins(response, textStatus, XMLHttpRequest) {
    var existing_divs = $("*[id^='div_edit_plugin_']")
    var existing_plugin_ids = new Array;
    for (var i = 0; i < existing_divs.length; i++) {
	console.log(existing_divs[i]);
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

// function success_progress_bar(response, textStatus, XMLHttpRequest) {
//     var clientid = response.percent;
//     var pluginsDiv = document.getElementById("test_diff");
//     pluginsDiv.innerHTML = "testes diff2 ok: "+clientid;
// }

function switch_select() {
    var hiddenEls  = $("div.selector").filter(":hidden");
    var visibleEls = $("div.selector").filter(":visible");
    $.each( hiddenEls, function(index, item){$(item).show();});
    $.each( visibleEls, function(index, item){$(item).hide();});
    if ($('div#local_config').attr('view_mode') == 'view'){
	$('input.switch_selector').attr('src', "img/serp_molot.png");
	$('input.switch_selector').attr('title', "Switch to view mode");
	$('div#local_config').attr('view_mode', 'edit');
    } else if ($('div#local_config').attr('view_mode') == 'edit'){
	$('input.switch_selector').attr('src', "img/graph_mode.jpg");
	$('input.switch_selector').attr('title', "Switch to edit mode");
	$('div#local_config').attr('view_mode', 'view');
    }
    
    return false;
}

function post_data_home( JSONstring, ctrl_funct) {
    $.ajax({
	type: "POST",
	url: "scripts/get_info.php",
	data: { json: JSON.stringify(JSONstring) },
	success: ctrl_funct,
	failure: function(errMsg) {alert(errMsg);}
    });
}

function get_customer_host_name() {
    var customer = $('div#local_config').attr('customer') || '';
    var hostname = $('div#local_config').attr('hostname') || '';
//     var url = $('#fileupload').prop('action');
//     if(typeof url === 'undefined'){return;};
//     var arr = url.split('?')[1].split('&');
//     var customer = arr[0].replace(/^customer=/, "");
//     var hostname = arr[1].replace(/^host=/, "");
    return [customer, hostname];
}

function update_blueimp_downloads() {
    // example result: from http://wikitiki.mindsoft.com/mindparser/scripts/upload.class.php?customer=w&host=test1
    $.ajax({
	url: $('#fileupload').prop('action'),
	dataType: 'json',
	context: $('#fileupload')[0] 
    }).done(function (result) {
	var count = 0,
	    existing_in_page = {};
	$.each($("tr.template-download"), function (index, item) {
	    existing_in_page[$(item).children("td").children("a").attr("title")] = {item_obj:item};
	});

	var remaining_on_site=new Array;
	$.each(result.files, function (index, item){
	    if (existing_in_page[item.name]){
		delete existing_in_page[item.name];
	    } else {
		remaining_on_site[count++] = item;
	    }
	});
	// remove deleted files
	$.each(existing_in_page, function (index, item){ $(existing_in_page[index].item_obj).remove();});
	// add new files
	$(this).fileupload('option', 'done').call(this, null, {result: {"files": remaining_on_site}});
    });
}

function updates() {
    var arr = get_customer_host_name();
//     console.log(arr);
    var JSONstring = { customer:arr[0], hostname:arr[1], data:{function:"get_plugins", extra:{}} };
    post_data_home(JSONstring, updatePlugins);
}

$(function() {
  $( "input.switch_selector" ).click(function () {switch_select();});
  updates();
  var t1=setInterval('updates()', 1000);
  // wait 3 seconds for the main upload script to finish
  setTimeout(function(){setInterval('update_blueimp_downloads()', 300)},3000);
});
