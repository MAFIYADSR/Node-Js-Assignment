const express = require('express');
const cluster = require('cluster');
const os = require('os');
const taskHandler = require('./taskHandler');

if (cluster.isMaster) {
  const numWorkers = 2; // Two replica sets as required
  for (let i = 0; i < numWorkers; i++) {
    cluster.fork();
  }
  cluster.on('exit', (worker) => {
    console.log(`Worker ${worker.process.pid} died. Spawning a new one.`);
    cluster.fork();
  });
} else {
  const app = express();
  app.use(express.json());

  app.post('/api/v1/task', taskHandler);

  const PORT = 3000;
  app.listen(PORT, () => {
    console.log(`Worker ${process.pid} listening on port ${PORT}`);
  });
}
