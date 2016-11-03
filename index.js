'use strict';

var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var argv = require('optimist').argv;
var topic = argv.topic || 'test';

function initConsumerAndReceiveMessages(kafkaIp,kafkaPort,kafkaTopic,eventraiser,eventname){
var server = kafkaIp + ':' + kafkaPort ;

console.log("initConsumerAndReceiveMessages");

var client = new Client(server);
var topics = [
    {topic: kafkaTopic, partition: 1},
    {topic: kafkaTopic, partition: 0}
];
var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

var consumer = new Consumer(client, topics, options);
var offset = new Offset(client);

consumer.on('message', function (message) {
  console.log(message);

    try{
       eventraiser.emit(eventname, message);
    } catch (err){
        console.log(err);
      
    }
});

consumer.on('error', function (err) {
  console.log('error', err);
});

/*
* If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
*/
consumer.on('offsetOutOfRange', function (topic) {
  topic.maxNum = 2;
  offset.fetch([topic], function (err, offsets) {
    if (err) {
      return console.error(err);
    }
    var min = Math.min(offsets[topic.topic][topic.partition]);
    consumer.setOffset(topic.topic, topic.partition, min);
  });
});


}

exports.initConsumerAndReceiveMessages = initConsumerAndReceiveMessages;