
var express = require('express');
var app = express();
var path = require('path');

var WebSocketClient = require('websocket').client;
var client = new WebSocketClient();


app.get('/', function(req, res) {
    res.sendFile(path.join(__dirname + '/index.html'));
});
 
 client.on('connectFailed', function(error) {
    console.log('Connect Error: ' + error.toString());
});
 
client.on('connect', function(connection) {
    console.log('WebSocket Client Connected');
	// cy.add({
		// group: 'nodes',
		// data: { weight: 75 },
		// position: { x: 200, y: 200 }
	// });
    connection.on('error', function(error) {
        console.log("Connection Error: " + error.toString());
    });
    connection.on('close', function() {
        console.log('echo-protocol Connection Closed');
    });
    connection.on('message', function(message) {
        if (message.type === 'utf8') {
            console.log("Received: '" + message.utf8Data + "'");
        }
    });

});

	function sendSignal() {
	console.log("Button pressed");
	};

client.connect('ws://localhost:8887/');

app.listen(8080);
