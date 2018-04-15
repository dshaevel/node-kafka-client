module.exports = {
  kafka: {
    serverHealth: {
      zkHostPorts: '127.0.0.1:2181',
      path: '/',
      autoCommit: false,
      fetchMaxWaitMs: 5000,
      fetchMaxBytes: 1048576,
      requireAcks: 1,
      retryOptions: {
        retries: 1,
        factor: 1,
        minTimeout: 5000,
        maxTimeout: 10000,
        randomize: false
      }
    }
  }
};