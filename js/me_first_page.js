"use strict";

function post_data_home( JSONstring, ctrl_funct) {
    $.ajax({
	type: "POST",
	url: "scripts/get_info.php",
	data: { json: JSON.stringify(JSONstring) },
	success: ctrl_funct,
	failure: function(errMsg) {alert(errMsg);}
    });
}

function get_customer_exists_success(response, textStatus, XMLHttpRequest) {
    if (response) {
	$( "a.select_customer" ).button({icons: {primary: 'ui-icon-circle-check'}});
    } else {
	$( "a.select_customer" ).button({icons: {primary: 'ui-icon-circle-plus'}});
    }
}

function create_dialog(div_id, link_name) {
    var arr = get_customer_host_name();
    var customer = arr[0],
	hostname = arr[1];

//     write_error_div("_"+customer+"_");
    if (customer === "") {
      $("a.add_host").hide();
    }
    $("div.inputdata").hide();
//     var table = '\
//   <table>\
//     <tbody>\
//       <td><a class="add_customer" onselectstart=\'return false;\'>Add new customer</a></td>\
//       <td><a class="add_host" onselectstart=\'return false;\'>Add new host</a></td>\
//     </tbody>\
//   </table>';
//     div_id = div_id.substring(1);
//     var div = $(document.createElement('div'))
// 	.addClass(div_id)
// 	.attr({ title : "Edit customer "+customer })
// 	.append(table);
//     $('div#edit_plugins_forms_placer').append(div);
//
//

    
//     <div id="div_edit_plugin_'+plugin_id+'" title="Edit stats for '+plugin_name+'" class="div_edit_plugin">\
//     var div = $.create("div").hide();
//     this.$OuterDiv = $('div')
//     .hide()

//     var d = $(document.createElement('div'));
//     $('div#edit_plugins_forms_placer').append(d);
//     .hide()
//     .append($('<table></table>')
//         .attr({ cellSpacing : 0 })
//         .addClass("text")
//     );

// <div id="edit_customers" title="Edit customer" style="display:none;">
//   <table class="edit_customers">
//     <tbody>
//       <tr>
// 	<td><label>Customer: </label></td>
// 	<td><input type="text" id="autocomplete_customers" class="defaultText" title="Enter customer" style="width:100%;float:left"/></td>
// 	<td><a class="select_customer" onselectstart=\'return false;\'>&hellip;</a></td>
//       </tr>
// 	  <td></td><td></td><td></td>
//       <tr>
//       </tr>
//     </tbody>
//   </table>
//   <table class="edit_customers">
//     <tbody>
// 	<td><a class="add_customer" onselectstart=\'return false;\'>Add new customer</a></td>
// 	<td><a class="add_host" onselectstart=\'return false;\'>Add new host</a></td>
//   </tbody>
//   </table>
// <!--  <input id="autocomplete_customers" class="defaultText" title="Enter customer" type="text" />
// <input id="autocomplete_hosts" class="defaultText" title="Enter hostname" type="text"/>-->
// </div>
}

function input_text() {
    $(".defaultText").focus(function(srcc){
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
    
    $(".defaultText").blur();
}

function test() {
  var div_id = "#edit_customers",
      link_name = "a.edit_menu";
  create_dialog(div_id, link_name);

  $( div_id ).dialog({
    autoOpen: false,
    height: 300,
    width: 350,
    modal: true,
    open: function(event, ui){
    },
    buttons: {
      Submit: function() {
      },
      Cancel: function() {
      $( this ).dialog( "close" );
      },
      'Delete customer': function() {
      },
    },
    close: function() {
    },
  });

  $( link_name ).click(function() {
      var arr = get_customer_host_name();
      var JSONstring = { customer:arr[0], hostname:'', data:{function:"get_hosts", extra:{} }};
      post_data_home(JSONstring, get_hosts_success);
      $( div_id ).dialog( "open" );
  });
  
  $( "a.add_customer" ).button().click(function() {
    $("a.add_host").hide();
    $("a.add_customer").hide();
    $("div.inputdata").show();
    complete_cust();
  });
  $( "a.add_host" ).button().click(function() {
  });
  $( "a.select_customer" ).button().click(function() {
    $("a.add_host").show();
    $("a.add_customer").show();
    $("div.inputdata").hide();

    write_error_div($( "#autocomplete_customers" ).result());
    var JSONstring = { customer:'', hostname:'', data:{function:$( "#autocomplete_customers" ).result(), extra:{} }};
    post_data_home(JSONstring, get_hosts_success);

    $("#autocomplete_customers").autocomplete("destroy");
    $("input#autocomplete_customers").val('');
    $( "a.select_customer" ).button({icons: {primary: ''}});
    $( "#combobox" ).next().val('');
    
  });

//   $( "a.add_customer" )
}


function complete_cust() {
    $( "#autocomplete_customers" )
    .bind( "keydown", function( event ) {
	if ( event.keyCode === $.ui.keyCode.TAB && $( this ).data( "autocomplete" ).menu.active ) {
	    event.preventDefault();
	}
    })
    .autocomplete({
	source: function( request, response ) {
// 	    write_error_div(request.term);
	    var JSONstring = { customer:"", hostname:"", data:{function:"get_customers_autocomplete", extra:{request:request.term}} };
	    post_data_home(JSONstring, response);
	    var JSONstring = { customer:request.term, hostname:"", data:{function:"get_customer_exists", extra:{}} };
	    post_data_home(JSONstring, get_customer_exists_success);
	},
	focus: function() {/*item is focused in the list*/return false;},
	select: function( event, ui ) {
	    get_customer_exists_success(true);
// 	    log( ui.item ? "Selected: " + ui.item.label : "Nothing selected, input was " + this.value);
	    },
	});
}

function get_hosts_success(response, textStatus, XMLHttpRequest) {
    write_error_div(JSON.stringify(response));
}

function get_customer_host_name() {
    var customer = $('div#local_config').attr('customer') || '';
    var hostname = $('div#local_config').attr('hostname') || '';
    return [customer, hostname];
}

$(function() {
  input_text();
  test();
});
