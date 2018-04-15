var Promise = require('bluebird');

var _conf = global.conf;
var _kafkaConf = global.conf && global.conf.kafka;

var _ = require('lodash');
var _kafka = require('kafka-node');
var _retry = require('retry');

var Client = _kafka.Client;
var Consumer = _kafka.Consumer;
var Offset = _kafka.Offset;
var Producer = _kafka.Producer;

var _consumerClientList = {};
var _producerClientList = {};
var _producerList = {};

var AUTOCOMMIT = false;
var FETCHMAXWAITMS = 5000;
var FETCHMAXBYTES = 1024 * 1024;
var REQUIREACKS = 1;

var RETRIES = 10;
var FACTOR = 2;
var MIN_TIMEOUT = 1000;
var MAX_TIMEOUT = Infinity;
var RANDOMIZE = false;

if (!_kafkaConf) {
	var errorMsg = 'Missing configuration for kafka';
	console.error(errorMsg);
	throw new Error(errorMsg);
}

/**
 * Creates a kafka client for the specified cluster name.
 *
 * @param clusterName The name of kafka cluster to create a client for. The
 * clusterName should correspond to a kafka entry in conf.js that specifies
 * the Zookeeper connection information for the cluster:
 *
 * ...
 * ...
 * kafka: {
 *   <clusterName>: {
 *     zkHostPorts: 'server1:2181, server2:2181, server3:2181',
 *     path: '/'
 *   }
 * }
 * ...
 * ...
 *
 * @returns a kafka client object, or null if Zookeeper connection
 * information is not found in conf.js
 *
 * @private
 */
function _createClient(clusterName) {
	var zkConnection;

	if (!clusterName) {
		console.error('Missing Kafka cluster name');
	}

	if (!_kafkaConf[clusterName]) {
		console.error('Missing configuration for Kafka cluster "' + clusterName + '"');
	}

	var zkHostPorts = _kafkaConf[clusterName] && _kafkaConf[clusterName].zkHostPorts;
	var zkPath = _kafkaConf[clusterName] && _kafkaConf[clusterName].path;

	if (zkHostPorts && zkPath) {
		zkConnection = zkHostPorts + zkPath;
	}

	console.info('zk ==>' + JSON.stringify(zkConnection, null, 2) + '<==');

	return zkConnection ? new Client(zkConnection) : null;
}

/**
 * For the given cluster name, either gets the existing kafka client or creates
 * a new kafka client (Singleton pattern).
 *
 * @param clusterName The name of the kafka cluster to get a client for.
 *
 * @returns a kafka client object
 *
 * @private
 */
function _getConsumerClient(clusterName) {
	if (!_consumerClientList[clusterName]) {
		_consumerClientList[clusterName] = _createClient(clusterName);
	}
	return _consumerClientList[clusterName];
}

/**
 * For the given cluster name, either gets the existing kafka client or creates
 * a new kafka client (Singleton pattern).
 *
 * @param clusterName The name of the kafka cluster to get a client for.
 *
 * @returns a kafka client object
 *
 * @private
 */
function _getProducerClient(clusterName) {
	if (!_producerClientList[clusterName]) {
		_producerClientList[clusterName] = _createClient(clusterName);
	}
	return _producerClientList[clusterName];
}

/**
 * Gets a kafka consumer object.
 *
 * @param clusterName
 * @param topicName
 * @param partitionNumber
 * @param offsetNumber
 * @param callback
 * @returns {*}
 */
function getConsumer(clusterName, topicName, partitionNumber, offsetNumber, callback) {
	var isConsumerReady = false;

	var client = _getConsumerClient(clusterName);
	if (!client) {
		return callback('Kafka client is undefined', null);
	}
	var partitionNum = partitionNumber || 0;
	var hasOffset = !!offsetNumber;
	var offsetNum = offsetNumber || 0;

	var autoCommit = _kafkaConf[clusterName].autoCommit ? _kafkaConf[clusterName].autoCommit : AUTOCOMMIT;
	var fetchMaxWaitMs = _kafkaConf[clusterName].fetchMaxWaitMs ? _kafkaConf[clusterName].fetchMaxWaitMs : FETCHMAXWAITMS;
	var fetchMaxBytes = _kafkaConf[clusterName].fetchMaxBytes ? _kafkaConf[clusterName].fetchMaxBytes : FETCHMAXBYTES;

	var options = { autoCommit: autoCommit, fetchMaxWaitMs: fetchMaxWaitMs, fetchMaxBytes: fetchMaxBytes, fromOffset: hasOffset };
	var consumer = new Consumer(client, [{topic: topicName, partition: partitionNum, offset: offsetNum}], options);

	consumer.on('offsetOutOfRange', function processOffsetOutOfRangeError() {
		var offset = new Offset(client);
		var payload = [{
			topic: topicName,
			partition: partitionNum,
			time: -1,
			maxNum: 1
		}];
		offset.fetch(payload, function processFetchedOffsets(err, offsets) {
			offsetNum = Math.min.apply(null, offsets[topicName][partitionNum]);
			consumer.setOffset(topicName, partitionNum, offsetNum);
		});
	});
	consumer.on('error', function processConsumerError(err) {
		console.error('Kafka consumer error: ' + err);
		if (!isConsumerReady) {
			isConsumerReady = true;
			return callback(err);
		}
	});

	isConsumerReady = true;
	console.info('Kafka consumer ready!');
	return callback(null, consumer);
}

/**
 * Gets a kafka offset object.
 *
 * @param clusterName
 * @param callback
 * @returns {*}
 */
function getOffset(clusterName, callback) {
	var client = _getConsumerClient(clusterName);
	if (!client) {
		return callback('Kafka client is undefined');
	}
	var offset = new Offset(client);
	return callback(null, offset);
}

/**
 * Gets a kafka producer object.
 *
 * @param clusterName
 * @param callback
 * @returns {*}
 */
function getProducer(clusterName, callback) {
	if (_producerList[clusterName]) {
		return callback(null, _producerList[clusterName]);
	}

	var getProducerOnReadyCalled = false;

	var client = _getProducerClient(clusterName);
	if (!client) {
		return callback('Kafka client is undefined');
	}

	var requireAcks = _kafkaConf[clusterName].requireAcks ? _kafkaConf[clusterName].requireAcks : REQUIREACKS;

	var producer = new Producer(client, { requireAcks: requireAcks });

	producer.on('error', function processProducerError(err) {
		console.error('Kafka producer error: ' + err);
		if (!getProducerOnReadyCalled) {
			return callback(err);
		}
	});
	producer.on('ready', function processProducerReady() {
		console.info('Kafka producer ready!');
		if (!getProducerOnReadyCalled) {
			getProducerOnReadyCalled = true;
			_producerList[clusterName] = producer;
			return callback(null, _producerList[clusterName]);
		}
	});
}

/**
 * Sends the given message as a string to the given topic on the given kafka
 * cluster.
 *
 * @param clusterName
 * @param topicName
 * @param {Object|Array|string} message -- expects string; converts object or array using JSON.stringify
 * @param sendMessageCallback
 */
function sendMessage(clusterName, topicName, message, sendMessageCallback) {
	var messageString = (_.isObject(message) || _.isArray(message)) ? JSON.stringify(message) : message;

	getProducer(clusterName, function workWithProducer(err, producer) {
		if (err || !producer) {
			return sendMessageCallback(err || 'Unable to get kafka producer');
		}

		var retryOptions = {
			retries: _kafkaConf[clusterName].retryOptions && _kafkaConf[clusterName].retryOptions.retries ? _kafkaConf[clusterName].retryOptions.retries : RETRIES,
			factor: _kafkaConf[clusterName].retryOptions && _kafkaConf[clusterName].retryOptions.factor ? _kafkaConf[clusterName].retryOptions.factor : FACTOR,
			minTimeout: _kafkaConf[clusterName].retryOptions && _kafkaConf[clusterName].retryOptions.minTimeout ? _kafkaConf[clusterName].retryOptions.minTimeout : MIN_TIMEOUT,
			maxTimeout: _kafkaConf[clusterName].retryOptions && _kafkaConf[clusterName].retryOptions.maxTimeout ? _kafkaConf[clusterName].retryOptions.maxTimeout : MAX_TIMEOUT,
			randomize: _kafkaConf[clusterName].retryOptions && _kafkaConf[clusterName].retryOptions.randomize ? _kafkaConf[clusterName].retryOptions.randomize : RANDOMIZE
		};
		var retryOperation = _retry.operation(retryOptions);
		retryOperation.attempt(function sendMessageWithRetry(currentAttempt) {
			producer.send(
				[{topic: topicName, messages: [messageString]}],
				function processSendResults(error, results) {
					if (retryOperation.retry(error)) {
						console.info('kafkaClient.sendMessage()::retry attempt #' + currentAttempt);
						return;
					}
					sendMessageCallback(error ? retryOperation.mainError() : null, results);
				}
			);
		});
	});
}

module.exports = {
	getConsumer: getConsumer,
	getOffset: getOffset,
	getProducer: getProducer,
	sendMessage: sendMessage
};
