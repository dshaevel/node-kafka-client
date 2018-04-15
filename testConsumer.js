global.conf = require('./conf');

var Promise = require('bluebird');
var kafka = Promise.promisifyAll(require('./kafkaClient'));

var clusterName = 'serverHealth';
var topicName = 'metric';
var partitionNumber = 0;
var offsetNumber = 0;

function getKafkaConsumer() {
	return kafka.getConsumerAsync(clusterName, topicName, partitionNumber, offsetNumber);
}

getKafkaConsumer().then(function processKafkaConsumer(consumer) {
	consumer.on('message', function processMessage(message) {
		console.log('message ==>' + JSON.stringify(message, null, 2) + '<==');
	});
});
