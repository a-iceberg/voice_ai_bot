const ari = require('ari-client');
const { config, logger } = require('./config');

function printUsage() {
  console.log('Usage: node outbound_call.js <number-or-endpoint> [--caller 3000] [--timeout 30]');
  console.log('Examples:');
  console.log('  node outbound_call.js 300');
  console.log('  node outbound_call.js 89161234567 --caller 3000');
  console.log('  node outbound_call.js PJSIP/300 --caller 3000 --timeout 20');
}

function parseArgs(argv) {
  const args = [...argv];
  const positionals = [];
  const options = {
    callerId: '3000',
    timeout: 30
  };

  while (args.length > 0) {
    const current = args.shift();

    if (current === '--caller') {
      options.callerId = args.shift();
      continue;
    }

    if (current === '--timeout') {
      options.timeout = Number(args.shift());
      continue;
    }

    if (current === '--help' || current === '-h') {
      options.help = true;
      continue;
    }

    positionals.push(current);
  }

  options.target = positionals[0];
  return options;
}

function normalizeEndpoint(target) {
  const value = String(target || '').trim();

  if (!value) {
    throw new Error('Target number or endpoint is required');
  }

  if (value.includes('/')) {
    return value;
  }

  return `PJSIP/${value}`;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.help || !options.target) {
    printUsage();
    process.exit(options.help ? 0 : 1);
  }

  if (!Number.isFinite(options.timeout) || options.timeout <= 0) {
    throw new Error('Timeout must be a positive number of seconds');
  }

  const endpoint = normalizeEndpoint(options.target);
  const client = await ari.connect(config.ARI_URL, config.ARI_USER, config.ARI_PASS);

  try {
    logger.info(
      `Creating outbound bot call: callerId=${options.callerId}, endpoint=${endpoint}, timeout=${options.timeout}`
    );

    const channel = await client.channels.originate({
      endpoint,
      app: config.ARI_APP,
      callerId: String(options.callerId || '3000'),
      timeout: options.timeout,
      formats: 'ulaw',
      variables: {
        BOT_OUTBOUND_CALL: '1',
        BOT_TARGET: String(options.target)
      }
    });

    console.log(`Call created: ${channel.id}`);
    console.log(`Caller ID: ${options.callerId}`);
    console.log(`Endpoint: ${endpoint}`);
  } finally {
    if (typeof client.close === 'function') {
      client.close();
    }
  }
}

main().catch((error) => {
  logger.error(`Outbound call failed: ${error.message}`);
  process.exit(1);
});
