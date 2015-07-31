var WebSocketClient = require('websocket').client;
var client = new WebSocketClient();

client.on('connectFailed', function(error) {
    console.log('Connect Error: ' + error);
});

client.on('connect', function(connection) {
    console.log('Connected...');

    /* setup pub-sub message queue(s) */
    var kafka = require('kafka-node'),
        client = new kafka.Client(),
        producer = new kafka.Producer(client);
    producer.on('ready', function () {
        console.log("producer ready...");

    });
    producer.on('error', function (err) {
        console.log("producer error: " + err);
    });

    connection.on('error', function(error) {
        console.log("Connection Error: " + error);
    });
    connection.on('close', function() {
        console.log('Connection Closed');
    });
    connection.on('message', function(message) {
        if (message.type == 'utf8') {
            var messageJSON = message.utf8Data;
            console.log('message: ' + messageJSON);
            var payloads = [{topic: "transactions", messages: messageJSON}];
            producer.send(payloads, function (err, data) {
                console.log("producer send:");
                if (err) console.log("error: " + err);
                //console.log("data: " + data);
            });
        }
    });

    /* subscribe to unconfirmed transactions feed */
    function subscribe() {
        if (connection.connected) {
            console.log("subscribing...")
            var SUBSCRIBE_MESSAGE = '{"op":"unconfirmed_sub"}'; // MUST have single quotes on the outside & double-quotes inside in order to get messages
            connection.send(SUBSCRIBE_MESSAGE); //
        }
    }
    subscribe();
});

client.connect('wss://ws.blockchain.info/inv');
