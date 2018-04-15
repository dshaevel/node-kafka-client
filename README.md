# node-kafka-client - v1.0.0 - David Shaevel
### 1/7/2016

---

## Description
A Node.js client for Apache Kafka. The node-kafka-client has the following four methods:

* getConsumer(clusterName, topicName, partitionNumber, offsetNumber, callback)
* getOffset(clusterName, callback)
* getProducer(clusterName, callback)
* sendMessage(clusterName, topicName, message, callback)

## Installation and Example Usage
    $ npm install

    ## In one terminal, start the test consumer:
    $ node testConsumer.js
    zk ==>"127.0.0.1:2181/"<==
    Kafka consumer ready!

    ## In another terminal, start the test producer and produce a message:
    $ node testProducer.js
    zk ==>"127.0.0.1:2181/"<==
    Kafka producer ready!
    results ==>{
      "metric": {
        "0": 0
      }
    }<==

    ## In the terminal running the test consumer, you will see the message:
    message ==>{
      "topic": "metric",
      "value": "{\"name\":\"MYSQL_READS\",\"displayName\":\"MySQL Read Operations\",\"description\":\"The number of MySQL read operations per seconds\",\"unit\":\"number\",\"displayNameShort\":\"mysql-reads\",\"d efaultAggregate\":\"AVG\",\"isDisabled\":false,\"isDeleted\":false,\"defaultResolutionMS\":1000,\"type\":\"system\"}","offset": 0,"partition": 0,"highWaterOffset": 1,"key": null
    }<==
---

## Prerequisites
### Java SE 8:
    $ brew cask install caskroom/versions/java8

### Apache Kafka and Apache Zookeeper (Kafka installation will install the Zookeeper dependency):
    $ brew install kafka

### Start Zookeeper:
    $ brew services start zookeeper
    ==> Successfully started `zookeeper` (label: homebrew.mxcl.zookeeper)

### Test Zookeeper:
    $ zkCli -server 127.0.0.1:2181
    [zk: 127.0.0.1:2181(CONNECTED) 0] ls /
    [zookeeper]
    [zk: 127.0.0.1:2181(CONNECTED) 1] quit
    Quitting...

### Start Kafka:
    $ brew services start kafka
    ==> Successfully started `kafka` (label: homebrew.mxcl.kafka)

### Test Kafka (create a topic named "metric"):
    $ kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic metric
    Created topic "metric".

    $ kafka-topics --list --zookeeper localhost:2181
    metric
