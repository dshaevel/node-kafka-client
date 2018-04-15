global.conf = require('./conf');

var Promise = require('bluebird');
var kafka = Promise.promisifyAll(require('./kafkaClient'));

var clusterName = 'serverHealth';
var topicName = 'metric';

var message = {
	name: 'MYSQL_READS',
	displayName: 'MySQL Read Operations',
	description: 'The number of MySQL read operations per seconds',
	unit: 'number',
	displayNameShort: 'mysql-reads',
	defaultAggregate: 'AVG',
	isDisabled: false,
	isDeleted: false,
	defaultResolutionMS: 1000,
	type: 'system'
};

function produceKafkaMessageOne() {
	return kafka.sendMessageAsync(clusterName, topicName, message);
}

produceKafkaMessageOne().then(function processResults(results) {
	console.log('results ==>' + JSON.stringify(results, null, 2) + '<==');
	process.exit(0);
}).catch(function processError(err) {
	console.error('error ==>' + err);
	process.exit(1);
});
