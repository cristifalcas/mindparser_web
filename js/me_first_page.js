"use strict";

function extend_dialog() {
    $.extend($.ui.dialog.prototype, {
      'addbutton': function(buttonName, func) {
	var buttons = this.element.dialog('option', 'buttons');
	buttons[buttonName] = func;
	this.element.dialog('option', 'buttons', buttons);
      }
    });

    $.extend($.ui.dialog.prototype, {
      'removebutton': function(buttonName) {
      var buttons = this.element.dialog('option', 'buttons');
	delete buttons[buttonName];
	this.element.dialog('option', 'buttons', buttons);
      }
    });
}

function post_data_home( JSONstring, ctrl_funct_success, ctrl_funct_error) {
    $.ajax({
	type: "POST",
	url: "scripts/get_info.php",
	data: { json: JSON.stringify(JSONstring) },
	timeout: 1000, // miliseconds
	success: ctrl_funct_success, // request, status, err
	error: function(request, status, err) {
            if(status == "timeout") {
                alert(status);
            } else if (status == "error") {
		var text = JSON.stringify(request);
		text = text.replace(/\\n/g, "\n");
		text = text.replace(/\\/g, "");
		alert("Error is \n"+text);
	    } else {
	      alert("Error status is "+status+" and "+JSON.stringify(request));
	      ctrl_funct_error(request, status, err);
	    }
        },
	failure: function(errMsg) {alert(errMsg);},
    });
}

// function get_customer_exists_success(response, textStatus, XMLHttpRequest) {
//     if (response) {
// 	$( "a.select_customer" ).button({icons: {primary: 'ui-icon-circle-check'}});
//     } else {
// 	$( "a.select_customer" ).button({icons: {primary: 'ui-icon-circle-plus'}});
//     }
// }

// function create_dialog(div_id, link_name) {
//     var arr = get_customer_host_name();
//     var customer = arr[0],
// 	hostname = arr[1];
// 
// //     write_error_div("_"+customer+"_");
//     if (customer === "") {
//       $("a.add_host").hide();
//     }
//     $("div.inputdata").hide();
// //     var table = '\
// //   <table>\
// //     <tbody>\
// //       <td><a class="add_customer" onselectstart=\'return false;\'>Add new customer</a></td>\
// //       <td><a class="add_host" onselectstart=\'return false;\'>Add new host</a></td>\
// //     </tbody>\
// //   </table>';
// //     div_id = div_id.substring(1);
// //     var div = $(document.createElement('div'))
// // 	.addClass(div_id)
// // 	.attr({ title : "Edit customer "+customer })
// // 	.append(table);
// //     $('div#edit_plugins_forms_placer').append(div);
// //
// //
// 
//     
// //     <div id="div_edit_plugin_'+plugin_id+'" title="Edit stats for '+plugin_name+'" class="div_edit_plugin">\
// //     var div = $.create("div").hide();
// //     this.$OuterDiv = $('div')
// //     .hide()
// 
// //     var d = $(document.createElement('div'));
// //     $('div#edit_plugins_forms_placer').append(d);
// //     .hide()
// //     .append($('<table></table>')
// //         .attr({ cellSpacing : 0 })
// //         .addClass("text")
// //     );
// 
// // <div id="edit_customers" title="Edit customer" style="display:none;">
// //   <table class="edit_customers">
// //     <tbody>
// //       <tr>
// // 	<td><label>Customer: </label></td>
// // 	<td><input type="text" id="autocomplete_customers" class="defaultText" title="Enter customer" style="width:100%;float:left"/></td>
// // 	<td><a class="select_customer" onselectstart=\'return false;\'>&hellip;</a></td>
// //       </tr>
// // 	  <td></td><td></td><td></td>
// //       <tr>
// //       </tr>
// //     </tbody>
// //   </table>
// //   <table class="edit_customers">
// //     <tbody>
// // 	<td><a class="add_customer" onselectstart=\'return false;\'>Add new customer</a></td>
// // 	<td><a class="add_host" onselectstart=\'return false;\'>Add new host</a></td>
// //   </tbody>
// //   </table>
// // <!--  <input id="autocomplete_customers" class="defaultText" title="Enter customer" type="text" />
// // <input id="autocomplete_hosts" class="defaultText" title="Enter hostname" type="text"/>-->
// // </div>
// }

function checkRegexp( o, regexp ) {
  if ( !( regexp.test( o.val() ) ) ) {
    o.addClass( "ui-state-error" );
    setTimeout(function() {o.removeClass( "ui-state-error", 700 );}, 300 );
    return false;
  } else {
    return true;
  }
}

function input_text() {
    $(".defaultText").focus(function(){
        if ($(this).val() == $(this)[0].title){
            $(this).removeClass("defaultTextActive");
            $(this).val("");
        }
    });
    
    $(".defaultText").blur(function(){
        if ($(this).val() == ""){
            $(this).addClass("defaultTextActive");
            $(this).val($(this)[0].title);
        }
    });
     $(".defaultText").addClass("ui-widget-content ui-corner-all");
    $(".defaultText").blur();
}

function validate_customer(){
  var correct = checkRegexp( $("input[title='Enter customer name']"), /^[a-z]([0-9a-z_])+$/i );
  $("table#hosts").children("tbody").children("tr:not(#add_new_host)").children("td").children("input[title='host name']").each(function (index) {
    correct = correct && checkRegexp($(this), /^[a-z]([0-9a-z_])+$/i);
  });
  return correct;
}

function clear_customer() {
  $("input[title='Enter customer name']").first().val('').blur();
  $("input[title='host name']").first().val('').blur();
  $("input[title='ip addr']").first().val('').blur();
  $("input[title='user']").first().val('').blur();
  $("input[title='pass']").first().val('').blur();

  $("table#hosts").children("tbody").children("tr:not(#add_new_host)").each(function (index) {
    $(this).remove();
  });
  $("input[title='Enter customer name']").attr("disabled", false);
}

function add_host_to_dialog(host, ip, user, pass) {
  $( "#hosts tbody" ).append(
'<tr class="new">\
  <td><input class="ui-widget-content ui-corner-all" type="text" size="18" value="'+host+'"/></td>\
  <td><input class="ui-widget-content ui-corner-all" type="text" size="15" value="'+ip+'"/></td>\
  <td><input class="ui-widget-content ui-corner-all" type="text" size="18" value="'+user+'"/></td>\
  <td><input class="ui-widget-content ui-corner-all" type="text" size="18" value="'+pass+'"/></td>\
</tr>'
  );
  $('<button />', {
    'class': 'rem_host_btn ',
    text: "Delete",
    title: "Delete",
    href: ''
  })
  .button({icons: {secondary: "ui-icon-circle-close"}, text: false})
  .appendTo($(".new"))
  .click(function(){ $(this).parent().remove(); });
  $(".new").removeClass("new");
}

function append_new_row_hosts(elem) {
  var crt_hosts = new Array();
  $("input[title='host name']").each(function (index) {
    crt_hosts[$(this).val()] = index;
  });
  
  var crt_host = new Array();

  $("#add_new_host :input").each(function (index) {
    if ($(this).attr('title') == 'host name'){
      crt_host['host'] = $(this).hasClass('defaultTextActive') ? '' : $(this).val();
    };
    if ($(this).attr('title') == 'ip addr'){crt_host['ip']=$(this).hasClass('defaultTextActive') ? '' : $(this).val()};
    if ($(this).attr('title') == 'user'){crt_host['user']=$(this).hasClass('defaultTextActive') ? '' : $(this).val()};
    if ($(this).attr('title') == 'pass'){crt_host['pass']=$(this).hasClass('defaultTextActive') ? '' : $(this).val()};
  });
  if ( typeof crt_host['host'] == 'undefined' || crt_host['host'] == '' || crt_hosts[crt_host['host']] > 0){
    return;
  };

  add_host_to_dialog(crt_host['host'], crt_host['ip'], crt_host['user'], crt_host['pass']);
  //clean up
  $("input[title='host name']").first().val('').blur();
  $("input[title='ip addr']").first().val('').blur();
  $("input[title='user']").first().val('').blur();
  $("input[title='pass']").first().val('').blur();
}

function fill_customer(btn_element, data_id) {
  $("a[id_cust='"+data_id+"']").each(function (index) {
      var host=$(this).attr("host_name");
      add_host_to_dialog(host, '', '', '', "bla");
//     console.log(index, data_id, host);
  });
  $("input[title='Enter customer name']").val(data_id).attr("disabled", true);
}

function delete_customer_fnct() {
    var JSONstring = { customer:$("input[title='Enter customer name']").first().val(), hostname:'', data:{function:"delete_customer", extra:{}} };
    post_data_home(JSONstring, delete_customer_success, delete_customer_error);
}

function test() {
  $('<button />', {
    'class': 'rem_host_btn ',
    text: "Add",
    title: "Add",
    href: ''
  })
  .button({icons: {secondary: "ui-icon-circle-check"}, text: false})
  .appendTo($("#add_new_host"))
  .click(function(){ append_new_row_hosts($(this)); });

  $("#add_new_host").keyup(function (e) {
    if (e.keyCode == 13) {
      append_new_row_hosts(e);
    }
  });

  $( ".add_customer_btn" )
    .button()
    .click(function() {
      clear_customer($(this));
      var data_id=$(this).attr('data-id');
      if (typeof data_id === 'undefined'){
	$("#edit_customer").dialog('removebutton', 'Delete customer');
      } else {
	$("#edit_customer").dialog('addbutton', 'Delete customer', delete_customer_fnct);
	fill_customer($(this), data_id);
      }
      $( "#edit_customer" ).dialog("open");
    });

  $( "#edit_customer" ).dialog({
    autoOpen: false,
    height: 400,
    width: 645,
    modal: true,
    open: function(event, ui){},
    buttons: {
      Submit: function() {
	if ( validate_customer($(this)) ){
	  var JSONstring = { customer:$("input[title='Enter customer name']").first().val(), hostname:'', data:{function:"check_customer", extra:{}} };
	  post_data_home(JSONstring, check_customer_success, check_customer_error);
	}
      },
      Cancel: function() {
	$( this ).dialog( "close" );
      },
      'Delete customer': function() {
	delete_customer_fnct();
      },
    },
    close: function() {},
  });
}

function check_customer_success(response, textStatus, XMLHttpRequest) {
//   console.log(response, textStatus);
  if (!response) {
    var customer = $("input[title='Enter customer name']").first().val();
    var hosts = new Array();
    $("table#hosts").children("tbody").children("tr:not(#add_new_host)").each(function (index) {
      var host, ip, user, pass;
      $(this).children("td").children(":input").each(function () {
	if ($(this).attr('title') == 'host name'){
	  host = $(this).hasClass('defaultTextActive') ? '' : $(this).val();
	};
	if ($(this).attr('title') == 'ip addr'){ip=$(this).hasClass('defaultTextActive') ? '' : $(this).val()};
	if ($(this).attr('title') == 'user'){user=$(this).hasClass('defaultTextActive') ? '' : $(this).val()};
	if ($(this).attr('title') == 'pass'){pass=$(this).hasClass('defaultTextActive') ? '' : $(this).val()};
      });
      hosts[index] = {host:host,ip:ip,user:user,pass:pass};
    });
//     console.log(hosts);
    var JSONstring = { customer: customer, hostname:'', data:{function:"add_customer", extra:{hosts:hosts}} };
    post_data_home(JSONstring, add_customer_success, add_customer_error);
  } else{
    alert("already exists");
  }
}

function add_customer_success(response, textStatus, XMLHttpRequest) {
  $( "#edit_customer" ).dialog("close");
}

function add_customer_error(response, textStatus, XMLHttpRequest) {
  alert("nok1");
}

function delete_customer_success(response, textStatus, XMLHttpRequest) {
  $( "#edit_customer" ).dialog("close");
}

function delete_customer_error(response, textStatus, XMLHttpRequest) {
  alert("nok2");
}
function check_customer_error(response, textStatus, XMLHttpRequest) {
  console.log(response, textStatus);
  alert("nok2");
}

// function complete_cust() {
//     $( "#autocomplete_customers" )
//     .bind( "keydown", function( event ) {
// 	if ( event.keyCode === $.ui.keyCode.TAB && $( this ).data( "autocomplete" ).menu.active ) {
// 	    event.preventDefault();
// 	}
//     })
//     .autocomplete({
// 	source: function( request, response ) {
// // 	    write_error_div(request.term);
// 	    var JSONstring = { customer:"", hostname:"", data:{function:"get_customers_autocomplete", extra:{request:request.term}} };
// 	    post_data_home(JSONstring, response);
// 	    var JSONstring = { customer:request.term, hostname:"", data:{function:"get_customer_exists", extra:{}} };
// 	    post_data_home(JSONstring, get_customer_exists_success);
// 	},
// 	focus: function() {/*item is focused in the list*/return false;},
// 	select: function( event, ui ) {
// 	    get_customer_exists_success(true);
// // 	    log( ui.item ? "Selected: " + ui.item.label : "Nothing selected, input was " + this.value);
// 	    },
// 	});
// }

// function get_hosts_success(response, textStatus, XMLHttpRequest) {
//     write_error_div(JSON.stringify(response));
// }

function get_customer_host_name() {
    var customer = $('div#local_config').attr('customer') || '';
    var hostname = $('div#local_config').attr('hostname') || '';
    return [customer, hostname];
}

$(function() {
  extend_dialog();
  $( "#accordion" ).accordion({
    animate: 100,
    heightStyle: "content",
    collapsible: true,
    active: false,
  });
  input_text();
  test();
});
