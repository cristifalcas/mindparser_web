/* enable strict mode */
"use strict";

// global variables
var progress,			// progress element reference
	request,			// request object
	// method definition
	initXMLHttpClient,	// create XMLHttp request object in a cross-browser manner
	send_request,		// send request to the server
	request_handler;	// request handler (started from send_request)

// define reference to the progress bar and create XMLHttp request object
window.onload = function () {
	progress = document.getElementById('progress');
	request = initXMLHttpClient();
};


// create XMLHttp request object in a cross-browser manner
initXMLHttpClient = function () {
	var XMLHTTP_IDS,
		xmlhttp,
		success = false,
		i;
	// Mozilla/Chrome/Safari/IE7+ (normal browsers)
	try {
		xmlhttp = new XMLHttpRequest(); 
	}
	// IE(?!)
	catch (e1) {
		XMLHTTP_IDS = [ 'MSXML2.XMLHTTP.5.0', 'MSXML2.XMLHTTP.4.0',
						'MSXML2.XMLHTTP.3.0', 'MSXML2.XMLHTTP', 'Microsoft.XMLHTTP' ];
		for (i = 0; i < XMLHTTP_IDS.length && !success; i++) {
			try {
				success = true;
				xmlhttp = new ActiveXObject(XMLHTTP_IDS[i]);
			}
			catch (e2) {}
		}
		if (!success) {
			throw new Error('Unable to create XMLHttpRequest!');
		}
	}
	return xmlhttp;
};


// send request to the server
send_request = function () {
// 		request.open('GET', 'scripts/progress-bar.php', true);	// open asynchronus request
		request.open('GET', 'index.php?fname=Peter&age=37&PROGRESS=new', true);	// open asynchronus request
		request.onreadystatechange = request_handler;		// set request handler	
		request.send(null);					// send request
};


// request handler (started from send_request)
request_handler = function () {
	var level;
	if (request.readyState === 4) { // if state = 4 (operation is completed)
		if (request.status === 200) { // and the HTTP status is OK
			// get progress from the XML node and set progress bar width and innerHTML
			level = request.responseXML.getElementsByTagName('PROGRESS')[0].firstChild;
			progress_bar.style.width = progress_bar.innerHTML = level.nodeValue + '%';
		}
		else { // if request status is not OK
			progress_bar.style.width = '100%';
			progress_bar.innerHTML = 'Error:[' + request.status + ']' + request.statusText;
		}
	} 
};


window.setInterval('send_request()', 1000); 
