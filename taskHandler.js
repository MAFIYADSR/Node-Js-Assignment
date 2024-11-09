const fs = require('fs').promises;
const Redis = require('ioredis');
const redis = new Redis();

const RATE_LIMIT_SECOND = 1;
const RATE_LIMIT_MINUTE = 20;

const rateLimitKey = (user_id, window) => `rate_limit:${user_id}:${window}`;
const queueKey = (user_id) => `task_queue:${user_id}`;

async function logTask(user_id) {
  const logMessage = `${user_id} - task completed at - ${new Date().toISOString()}\n`;
  await fs.appendFile('task.log', logMessage);
  console.log(logMessage);
}

async function taskHandler(req, res) {
  const { user_id } = req.body;
  if (!user_id) return res.status(400).send({ error: 'User ID is required.' });

  const [secLimit, minLimit] = await Promise.all([
    redis.incr(rateLimitKey(user_id, 'second')),
    redis.incr(rateLimitKey(user_id, 'minute'))
  ]);

  if (secLimit === 1) await redis.expire(rateLimitKey(user_id, 'second'), 1);
  if (minLimit === 1) await redis.expire(rateLimitKey(user_id, 'minute'), 60);

  if (secLimit > 1 || minLimit > RATE_LIMIT_MINUTE) {
    await redis.rpush(queueKey(user_id), JSON.stringify({ user_id }));
    return res.status(429).send({ message: 'Rate limit exceeded. Task queued.' });
  }

  processTask(user_id);
  res.status(200).send({ message: 'Task is being processed.' });
}

async function processTask(user_id) {
  setTimeout(async () => {
    await logTask(user_id);
    await processQueuedTask(user_id);
  }, 1000);
}

async function processQueuedTask(user_id) {
  const queuedTask = await redis.lpop(queueKey(user_id));
  if (queuedTask) {
    const { user_id: queuedUserId } = JSON.parse(queuedTask);
    setTimeout(async () => {
      await logTask(queuedUserId);
      await processQueuedTask(queuedUserId);
    }, 1000);
  }
}

module.exports = taskHandler;
