const express = require('express');
const rateLimit = require('express-rate-limit');
const Queue = require('bull');
const fs = require('fs');
const cluster = require('cluster');
const os = require('os');

const taskQueue = new Queue('taskQueue');
const numCPUs = os.cpus().length;

if (cluster.isMaster) {
  for (let i = 0; i < Math.min(numCPUs, 2); i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker) => {
    console.log(`Worker ${worker.process.pid} died. Forking a new one.`);
    cluster.fork();
  });
} else {
  const app = express();
  app.use(express.json());

  const limiter = rateLimit({
    windowMs: 60 * 1000,
    max: 20,
    keyGenerator: (req) => req.body.user_id,
  });

  app.use('/api/v1/task', limiter);

  taskQueue.process(async (job, done) => {
    await task(job.data.user_id);
    done();
  });

  app.post('/api/v1/task', async (req, res) => {
    const { user_id } = req.body;
    if (!user_id) {
      return res.status(400).send('user_id is required');
    }
    taskQueue.add({ user_id });
    res.status(200).send('Task queued');
  });

  async function task(user_id) {
    const logMessage = `${user_id} - task completed at ${new Date().toISOString()}\n`;
    fs.appendFile('task_log.txt', logMessage, (err) => {
      if (err) console.error('Error writing to log file', err);
    });
    console.log(logMessage);
  }

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log(`Server running on port ${PORT}, worker ${process.pid}`));
}
