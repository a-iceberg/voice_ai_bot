// ari_app_with_rtp.js

// Import required Node.js modules for ARI, promisification, logging, and RTP handling
const ari = require('ari-client');           // Library to interact with Asterisk REST Interface (ARI)
const { promisify } = require('util');       // Utility to convert callback-based functions to promises
const winston = require('winston');          // Logging library for structured output
const RTPServer = require('./rtpServer');    // Custom module for RTP server functionality (recording/playback)

// Logger configuration: outputs logs to console and a file with a max size of 1MB
const logger = winston.createLogger({
  level: 'info',                             // Log level set to 'info' (can be adjusted to 'debug', 'error', etc.)
  format: winston.format.combine(            // Combine multiple format options
    winston.format.timestamp(),              // Add timestamp to each log entry
    winston.format.printf(i => `[${i.timestamp}] ${i.level.toUpperCase()}: ${i.message}`) // Format log as [TIMESTAMP] LEVEL: MESSAGE
  ),
  transports: [                              // Define where logs are output
    new winston.transports.Console(),        // Output logs to the console
    new winston.transports.File({ filename: 'app.log', maxsize: 1024 * 1024, maxFiles: 1 }) // Output to 'app.log', max 1MB, 1 file
  ]
});

// Handle uncaught exceptions to log them and prevent crashes
process.on('uncaughtException', (err) => {
  logger.error(`Uncaught error: ${err.stack}`); // Log the error with its full stack trace
});

// ARI connection details
const ARI_URL  = 'http://127.0.0.1:8088',    // URL where Asterisk's ARI server is running (localhost:8088)
      ARI_USER = 'asterisk',                 // ARI username for authentication
      ARI_PASS = 'asterisk',                 // ARI password for authentication
      ARI_APP  = 'stasis_app';               // Name of the ARI application to handle calls

const RTP_PORT = 12000;                      // Port number for the RTP server to listen on

// Maps to track external media channels and SIP channels with their bridges
const extMap = new Map(),                    // Map to store external media channel IDs and their bridge IDs
      sipMap = new Map();                    // Map to store SIP channel IDs and their bridge objects
let rtpServerInstance = null;                // Singleton instance of the RTP server, initially null

// Function to add an externalMedia channel to a bridge, with retry logic
async function addExtToBridge(client, channel, bridgeId, retries = 5, delay = 500) {
  try {
    const bridge = await promisify(client.bridges.get).bind(client.bridges)({ bridgeId }); // Get the bridge by ID
    if (!bridge) throw new Error('Bridge not found'); // Throw error if bridge doesn't exist
    await promisify(bridge.addChannel).bind(bridge)({ channel: channel.id }); // Add the channel to the bridge
    logger.info(`ExternalMedia channel ${channel.id} added to bridge ${bridgeId}`); // Log success
  } catch (err) {
    if (retries) { // If retries remain
      logger.info(`Retrying to add externalMedia channel ${channel.id} to bridge ${bridgeId} (${retries} attempts remaining)`); // Log retry attempt
      await new Promise(r => setTimeout(r, delay)); // Wait for specified delay (500ms default)
      return addExtToBridge(client, channel, bridgeId, retries - 1, delay); // Recursively retry with one less attempt
    }
    logger.error(`Error adding externalMedia channel ${channel.id} to bridge ${bridgeId}: ${err}`); // Log failure after all retries
  }
}

// Main async function to initialize and run the ARI application
(async () => {
  try {
    // Connect to the ARI server using provided credentials
    const client = await new Promise((res, rej) => {
      ari.connect(ARI_URL, ARI_USER, ARI_PASS, (err, client) =>
        err ? rej(err) : res(client) // Resolve with client on success, reject with error on failure
      );
    });
    await promisify(client.start).bind(client)(ARI_APP); // Start the ARI application with the specified name
    logger.info(`ARI application "${ARI_APP}" started.`); // Log that the ARI app has started

    // Event handler for when a channel enters Stasis (call starts)
    client.on('StasisStart', async (evt, channel) => {
      if (channel.name && channel.name.startsWith('UnicastRTP')) { // Check if the channel is an external media channel
        logger.info(`ExternalMedia channel started: ${channel.id}`); // Log external media channel start
        let mapping = extMap.get(channel.id); // Get bridge mapping for this channel
        if (!mapping) { await new Promise(r => setTimeout(r, 500)); mapping = extMap.get(channel.id); } // Wait 500ms if mapping not found, then retry
        if (mapping) await addExtToBridge(client, channel, mapping.bridgeId); // Add channel to bridge if mapping exists
        return; // Exit handler for external media channels
      }
      logger.info(`SIP channel started: ${channel.id}`); // Log SIP channel start
      try {
        const bridge = await promisify(client.bridges.create).bind(client.bridges)({ type: 'mixing,proxy_media' }); // Create a mixing bridge
        await promisify(bridge.addChannel).bind(bridge)({ channel: channel.id }); // Add SIP channel to bridge
        sipMap.set(channel.id, bridge); // Map SIP channel to its bridge

        await promisify(channel.answer).bind(channel)(); // Answer the incoming call
        logger.info(`Channel ${channel.id} answered`); // Log that the call is answered

        if (!rtpServerInstance) { // If no RTP server instance exists
          const filename = `call_${Date.now()}.wav`; // Generate a unique WAV filename with timestamp
          rtpServerInstance = new RTPServer({ port: RTP_PORT, outFile: filename, logInterval: 500 }); // Create RTP server instance
          rtpServerInstance.start(); // Start the RTP server for recording
          logger.info(`RTP server started on port ${RTP_PORT}, recording to ${filename}`); // Log RTP server start

          // Schedule recording stop and playback after 10 seconds
          setTimeout(async () => {
            if (rtpServerInstance && sipMap.size > 0) { // Check if server exists and call is active
              rtpServerInstance.stop(); // Stop the RTP server
              logger.info(`Recording stopped after 10 seconds, file saved: ${filename}`); // Log recording stop
              await new Promise(resolve => setTimeout(resolve, 500)); // Wait 500ms to ensure file closure
              logger.info('Starting playback...'); // Log playback start
              rtpServerInstance.playback(); // Play back the recorded audio
            }
          }, 10000); // 10 seconds delay
        }

        // Parameters for creating an external media channel
        const extParams = {
          app: ARI_APP,                          // ARI application name
          external_host: `127.0.0.1:${RTP_PORT}`, // Host and port for RTP communication
          format: 'ulaw',                        // Audio format (μ-law)
          transport: 'udp',                      // UDP transport protocol
          encapsulation: 'rtp',                  // RTP encapsulation
          connection_type: 'client',             // Act as client for external media
          direction: 'both'                      // Bidirectional audio flow
        };
        const extChannel = await promisify(client.channels.externalMedia).bind(client.channels)(extParams); // Create external media channel
        extMap.set(extChannel.id, { bridgeId: bridge.id }); // Map external channel to bridge
        logger.info(`ExternalMedia channel ${extChannel.id} created and mapped to bridge ${bridge.id}`); // Log channel creation
      } catch (e) {
        logger.error(`Error in SIP channel ${channel.id}: ${e}`); // Log any errors during SIP channel handling
      }
    });

    // Event handler for when a channel leaves Stasis (call ends)
    client.on('StasisEnd', async (evt, channel) => {
      if (channel.name && channel.name.startsWith('UnicastRTP')) { // Check if it's an external media channel
        extMap.delete(channel.id); // Remove external channel from map
        logger.info(`ExternalMedia channel ${channel.id} removed from map`); // Log removal
      } else {
        const bridge = sipMap.get(channel.id); // Get bridge for SIP channel
        if (bridge) { // If bridge exists
          try {
            await promisify(bridge.destroy).bind(bridge)(); // Destroy the bridge
            logger.info(`Bridge ${bridge.id} destroyed`); // Log bridge destruction
          } catch (e) {
            logger.error(`Error destroying bridge ${bridge.id}: ${e}`); // Log destruction error
          }
          sipMap.delete(channel.id); // Remove SIP channel from map
        }
        if (sipMap.size === 0 && rtpServerInstance) { // If no active SIP channels remain
          rtpServerInstance.stop(); // Stop the RTP server
          rtpServerInstance = null; // Reset RTP server instance
          logger.info('RTP server stopped as no active channels remain.'); // Log server stop
        }
      }
      logger.info(`Channel ended: ${channel.id}`); // Log channel end
    });

    // Event handler for ARI client errors
    client.on('error', (err) => {
      logger.error(`ARI client error: ${err.message}`); // Log client errors
    });

    // Event handler for WebSocket connection closure
    client.on('close', () => {
      logger.info('WebSocket connection closed'); // Log WebSocket closure
    });

  } catch (err) {
    logger.error(`ARI connection error: ${err}`); // Log connection errors during startup
  }
})();
