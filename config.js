const DailyRotateFile = require('winston-daily-rotate-file');

const fs = require('fs');
const path = require('path');

require('dotenv').config({ path: './config.conf' });
const winston = require('winston');
const chalk = require('chalk');

// читаем data/auth.json 
const authPath = path.join(__dirname, 'data', 'auth.json');
let auth = {};
try {
  auth = JSON.parse(fs.readFileSync(authPath, 'utf8'));
} catch (e) {
  console.error(`Не удалось прочитать ${authPath}:`, e.message);
  process.exit(1);
}
// конфиг
const config = {
  ARI_URL: process.env.ARI_URL || 'http://127.0.0.1:8088',
  ARI_USER: auth.ARI_USER || process.env.ARI_USERNAME || 'asterisk',
  ARI_PASS: auth.ARI_PASS || process.env.ARI_PASSWORD || 'asterisk',
  ARI_APP: 'stasis_app',
  OPENAI_API_KEY: auth.OPENAI_API_KEY || process.env.OPENAI_API_KEY,
  REALTIME_URL: `wss://api.openai.com/v1/realtime?model=${process.env.REALTIME_MODEL || 'gpt-4o-mini-realtime-preview-2024-12-17'}`,
  RTP_PORT_START: 12000,
  MAX_CONCURRENT_CALLS: parseInt(process.env.MAX_CONCURRENT_CALLS) || 10,
  VAD_THRESHOLD: parseFloat(process.env.VAD_THRESHOLD) || 0.6,
  VAD_PREFIX_PADDING_MS: Number(process.env.VAD_PREFIX_PADDING_MS) || 200,
  VAD_SILENCE_DURATION_MS: Number(process.env.VAD_SILENCE_DURATION_MS) || 600,
  LOG_LEVEL: process.env.LOG_LEVEL || 'info',
  SYSTEM_PROMPT: process.env.SYSTEM_PROMPT,
  INITIAL_MESSAGE: process.env.INITIAL_MESSAGE || 'Hi',
  SILENCE_PADDING_MS: parseInt(process.env.SILENCE_PADDING_MS) || 100,
  CALL_DURATION_LIMIT_SECONDS: parseInt(process.env.CALL_DURATION_LIMIT_SECONDS) || 0, // 0 means no limit
  MAX_VALIDATION_RETRIES: parseInt(process.env.MAX_VALIDATION_RETRIES) || 3
};

let sentEventCounter = 0;
let receivedEventCounter = -1;

// единый препроцессор: считает счётчики и красит сообщение (для консоли),
// а для файла оставляем «чистый» текст без ANSI
const withCounters = winston.format((info) => {
  const msg = String(info.message);
  const origin = msg.split(' ', 1)[0];

  let counter;
  if (origin === '[Client]') {
    counter = `C-${sentEventCounter.toString().padStart(4, '0')}`;
    sentEventCounter++;
    info.messageColored = chalk.cyanBright(msg);
  } else if (origin === '[OpenAI]') {
    counter = `O-${receivedEventCounter.toString().padStart(4, '0')}`;
    receivedEventCounter++;
    info.messageColored = chalk.yellowBright(msg);
  } else {
    counter = 'N/A';
    info.messageColored = chalk.gray(msg);
  }
  info.counter = counter;
  info.messagePlain = msg; // для файлового вывода
  return info;
});

// формат для консоли
const consoleFormat = winston.format.combine(
  winston.format.timestamp(),
  withCounters(),
  winston.format.printf(({ timestamp, level, counter, messageColored }) =>
    `${counter} | ${timestamp} [${level.toUpperCase()}] ${messageColored}`
  )
);

// формат для файла (без цветов)
const fileFormat = winston.format.combine(
  winston.format.timestamp(),
  withCounters(),
  winston.format.printf(({ timestamp, level, counter, messagePlain }) =>
    `${counter} | ${timestamp} [${level.toUpperCase()}] ${messagePlain}`
  )
);

const logger = winston.createLogger({
  level: config.LOG_LEVEL,
  transports: [
    // консоль, как и раньше — с цветами
    new winston.transports.Console({ format: consoleFormat }),

    // файл с дневной ротацией
    new DailyRotateFile({
      dirname: 'logs',
      filename: 'app-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      zippedArchive: true,
      maxSize: '20m',
      maxFiles: '14d',
      format: fileFormat
    })
  ],
  // отдельные файлы для неотловленных исключений/промисов
  exceptionHandlers: [
    new DailyRotateFile({
      dirname: 'logs',
      filename: 'exceptions-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      zippedArchive: true,
      maxSize: '10m',
      maxFiles: '14d',
      format: fileFormat
    })
  ],
  rejectionHandlers: [
    new DailyRotateFile({
      dirname: 'logs',
      filename: 'rejections-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      zippedArchive: true,
      maxSize: '10m',
      maxFiles: '14d',
      format: fileFormat
    })
  ]
});

const logClient = (msg, level = 'info') => logger[level](`[Client] ${msg}`);
const logOpenAI = (msg, level = 'info') => logger[level](`[OpenAI] ${msg}`);

if (!config.SYSTEM_PROMPT || config.SYSTEM_PROMPT.trim() === '') {
  logger.error('SYSTEM_PROMPT is missing or empty in config.conf');
  process.exit(1);
}
logger.info('SYSTEM_PROMPT loaded from config.conf');

if (config.CALL_DURATION_LIMIT_SECONDS < 0) {
  logger.error('CALL_DURATION_LIMIT_SECONDS cannot be negative in config.conf');
  process.exit(1);
}
logger.info(`CALL_DURATION_LIMIT_SECONDS set to ${config.CALL_DURATION_LIMIT_SECONDS} seconds`);

module.exports = {
  config,
  logger,
  logClient,
  logOpenAI
};
