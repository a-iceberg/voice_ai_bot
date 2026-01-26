const { initializeAriClient } = require('./asterisk');
const { logger } = require('./config');

process.on('uncaughtException', (err) => {
  logger.error('UNCAUGHT EXCEPTION', err);
});

process.on('unhandledRejection', (reason) => {
  logger.error('UNHANDLED REJECTION', reason);
});

process.on('SIGTERM', () => {
  logger.warn('SIGTERM received, shutting down...');
  process.exit(0);
});

process.on('SIGINT', () => {
  logger.warn('SIGINT received, shutting down...');
  process.exit(0);
});

async function startApplication() {
  try {
    logger.info('Starting application');
    await initializeAriClient();
    logger.info('Application started successfully');

    await new Promise(() => {});
  } catch (e) {
    logger.error('Startup error', e);
    process.exit(1);
  }
}

startApplication();
