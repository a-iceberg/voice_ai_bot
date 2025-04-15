// Import required Node.js modules
const ari = require('ari-client'); // Asterisk REST Interface client
const WebSocket = require('ws'); // WebSocket library for OpenAI real-time API
const fs = require('fs'); // File system module for saving audio files
const dgram = require('dgram'); // UDP datagram for RTP audio streaming
const winston = require('winston'); // Logging library
const chalk = require('chalk'); // Colorizes console output
const async = require('async'); // Async utilities (used for RTP queue)
require('dotenv').config(); // Loads environment variables from .env file

// Configuration constants loaded from environment variables or defaults
const ARI_URL = 'http://192.168.1.82:8088'; // Asterisk ARI endpoint
const ARI_USER = 'asterisk'; // ARI username
const ARI_PASS = 'asterisk'; // ARI password
const ARI_APP = 'stasis_app'; // Stasis application name
const OPENAI_API_KEY = process.env.OPENAI_API_KEY; // OpenAI API key from .env
const REALTIME_URL = 'wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17'; // OpenAI real-time WebSocket URL
const RTP_PORT = 12000; // Local port for RTP audio reception
const MAX_CALL_DURATION = process.env.MAX_CALL_DURATION ? parseInt(process.env.MAX_CALL_DURATION) : 300000; // Max call duration in ms (default: 5 min)
const RTP_QUEUE_CONCURRENCY = parseInt(process.env.RTP_QUEUE_CONCURRENCY) || 50; // Concurrent RTP packet sends
const LOG_RTP_EVERY_N_PACKETS = parseInt(process.env.LOG_RTP_EVERY_N_PACKETS) || 100; // Log RTP stats every N packets
const ENABLE_RTP_LOGGING = process.env.ENABLE_RTP_LOGGING === 'true'; // Enable detailed RTP logging
const ENABLE_SENT_TO_OPENAI_RECORDING = process.env.ENABLE_SENT_TO_OPENAI_RECORDING === 'true'; // Controls saving of both .raw and .wav files
const VAD_THRESHOLD = process.env.VAD_THRESHOLD ? parseFloat(process.env.VAD_THRESHOLD) : 0.1; // Voice Activity Detection threshold
const VAD_PREFIX_PADDING_MS = process.env.VAD_PREFIX_PADDING_MS ? parseInt(process.env.VAD_PREFIX_PADDING_MS) : 300; // VAD prefix padding in ms
const VAD_SILENCE_DURATION_MS = process.env.VAD_SILENCE_DURATION_MS ? parseInt(process.env.VAD_SILENCE_DURATION_MS) : 500; // VAD silence duration in ms
const TARGET_RMS = 0.15; // Target Root Mean Square for audio normalization
const MIN_RMS = 0.001; // Minimum RMS to apply gain

// Counters for client/server event logging
let sentEventCounter = 0; // Tracks sent events to OpenAI
let receivedEventCounter = -1; // Tracks received events from OpenAI

// Configure Winston logger with timestamp and colorized output
const logger = winston.createLogger({
  level: 'info', // Log level
  format: winston.format.combine(
    winston.format.timestamp(), // Add timestamp to logs
    winston.format.printf(({ timestamp, level, message }) => {
      const [origin] = message.split(' ', 1); // Extract message origin (Client/Server)
      let counter;
      let coloredMessage;
      if (origin === '[Client]') {
        counter = `C-${sentEventCounter.toString().padStart(4, '0')}`; // Client event counter
        sentEventCounter++;
        coloredMessage = chalk.cyanBright(message); // Cyan for client messages
      } else if (origin === '[Server]') {
        counter = `S-${receivedEventCounter.toString().padStart(4, '0')}`; // Server event counter
        receivedEventCounter++;
        coloredMessage = chalk.yellowBright(message); // Yellow for server messages
      } else {
        counter = 'N/A'; // No counter for general logs
        coloredMessage = chalk.gray(message); // Gray for general logs
      }
      return `${counter} | ${timestamp} [${level.toUpperCase()}] ${coloredMessage}`; // Formatted log line
    })
  ),
  transports: [new winston.transports.Console()] // Output logs to console
});

// Helper functions for logging OpenAI events
const logClient = (msg) => logger.info(`[Client] ${msg}`); // Log client-side OpenAI events
const logServer = (msg) => logger.info(`[Server] ${msg}`); // Log server-side OpenAI events

// Maps to track channel states and audio buffers
const extMap = new Map(); // Maps ExternalMedia channels to their bridges and SIP channels
const sipMap = new Map(); // Maps SIP channels to their WebSocket and bridge data
const rtpSender = dgram.createSocket('udp4'); // Single UDP socket for sending RTP packets
let rtpReceiver = dgram.createSocket('udp4'); // UDP socket for receiving RTP packets
let ariClient; // ARI client instance

const audioFromAsteriskMap = new Map(); // Buffers audio received from Asterisk
const audioToOpenAIMap = new Map(); // Buffers audio sent to OpenAI
const amplificationLogFrequency = new Map(); // Tracks last amplification log time per channel
const rmsLogFrequency = new Map(); // Tracks last RMS log time per channel
const rtpSentStats = new Map(); // Tracks RTP stats per channel

// Add an ExternalMedia channel to a bridge with retry logic
async function addExtToBridge(client, channel, bridgeId, retries = 5, delay = 500) {
  try {
    const bridge = await client.bridges.get({ bridgeId }); // Fetch bridge by ID
    if (!bridge) throw new Error('Bridge not found');
    await bridge.addChannel({ channel: channel.id }); // Add channel to bridge
    logger.info(`ExternalMedia channel ${channel.id} added to bridge ${bridgeId}`);
  } catch (err) {
    if (retries) {
      logger.info(`Retrying to add ExternalMedia channel ${channel.id} to bridge ${bridgeId} (${retries} attempts remaining)`);
      await new Promise(r => setTimeout(r, delay)); // Wait before retrying
      return addExtToBridge(client, channel, bridgeId, retries - 1, delay); // Recursive retry
    }
    logger.error(`Error adding ExternalMedia channel ${channel.id} to bridge ${bridgeId}: ${err.message}`);
  }
}

// Start the RTP receiver to listen for audio from Asterisk
function startRTPReceiver() {
  let packetCount = 0; // Count of received RTP packets
  let totalBytes = 0; // Total bytes received
  let startTime = Date.now(); // Start time for rate calculation
  const audioBuffers = new Map(); // Temporary audio buffers per channel
  const BUFFER_INTERVAL_MS = 200; // Interval to process audio chunks (ms)

  rtpReceiver.on('listening', () => {
    const address = rtpReceiver.address();
    logger.info(`RTP Receiver listening on ${address.address}:${address.port}`);
  });

  // Handle incoming RTP packets
  rtpReceiver.on('message', (msg, rinfo) => {
    packetCount++;
    totalBytes += msg.length;
    if (packetCount >= 100) { // Log stats every 100 packets
      const currentTime = Date.now();
      const duration = (currentTime - startTime) / 1000;
      const rate = (packetCount / duration).toFixed(2);
      logger.info(`Received ${packetCount} RTP packets from ${rinfo.address}:${rinfo.port}, total bytes: ${totalBytes}, rate: ${rate} packets/s`);
      packetCount = 0;
      totalBytes = 0;
      startTime = currentTime;
    }

    // Find channel ID based on RTP source
    const channelId = [...sipMap.entries()].find(([_, data]) => data.rtpSource && data.rtpSource.address === rinfo.address && data.rtpSource.port === rinfo.port)?.[0];
    if (channelId) {
      const muLawData = msg.slice(12); // Extract μ-law payload (skip RTP header)
      if (!audioFromAsteriskMap.has(channelId)) audioFromAsteriskMap.set(channelId, Buffer.alloc(0));
      audioFromAsteriskMap.set(channelId, Buffer.concat([audioFromAsteriskMap.get(channelId), muLawData])); // Append to Asterisk audio buffer

      const pcmBuffer24kHz = muLawToPcm24kHz(muLawData, channelId); // Convert to PCM 24kHz
      if (!audioBuffers.has(channelId)) audioBuffers.set(channelId, Buffer.alloc(0));
      audioBuffers.set(channelId, Buffer.concat([audioBuffers.get(channelId), pcmBuffer24kHz])); // Append to temporary buffer

      // Set up interval to send audio to OpenAI
      if (!sipMap.get(channelId).sendTimeout) {
        sipMap.get(channelId).sendTimeout = setInterval(() => {
          const buffer = audioBuffers.get(channelId);
          if (buffer && buffer.length > 0) {
            let sumSquares = 0;
            for (let i = 0; i < buffer.length / 2; i++) { // Calculate RMS
              const sample = buffer.readInt16LE(i * 2);
              sumSquares += sample * sample;
            }
            const rms = Math.sqrt(sumSquares / (buffer.length / 2)) / 32768;
            const now = Date.now();
            if (rms < TARGET_RMS && rms > MIN_RMS) { // Normalize audio if RMS is low
              const gain = Math.min(TARGET_RMS / rms, 2);
              for (let i = 0; i < buffer.length / 2; i++) {
                let sample = buffer.readInt16LE(i * 2);
                sample = Math.round(sample * gain);
                sample = Math.max(-32768, Math.min(32767, sample));
                buffer.writeInt16LE(sample, i * 2);
              }
              if (!rmsLogFrequency.has(channelId) || now - rmsLogFrequency.get(channelId) >= 2000) {
                logger.info(`Adjusted RMS from ${rms.toFixed(3)} to ~${TARGET_RMS} with gain ${gain.toFixed(2)} for channel ${channelId}`);
                rmsLogFrequency.set(channelId, now);
              }
            }

            const base64Audio = buffer.toString('base64'); // Convert to base64 for OpenAI
            const channelData = sipMap.get(channelId);
            if (channelData && channelData.ws && channelData.ws.readyState === WebSocket.OPEN) {
              channelData.ws.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: base64Audio })); // Send to OpenAI
              if (!rmsLogFrequency.has(channelId) || now - rmsLogFrequency.get(channelId) >= 2000) {
                logClient(`Sending audio chunk to OpenAI for channel ${channelId} | Size: ${(buffer.length / 1024).toFixed(2)} KB | RMS: ${rms.toFixed(3)}`);
                rmsLogFrequency.set(channelId, now);
              }
            }
            audioBuffers.set(channelId, Buffer.alloc(0)); // Clear buffer after sending
          }
        }, BUFFER_INTERVAL_MS);
      }
    }
  });

  rtpReceiver.on('error', (err) => {
    logger.error(`RTP Receiver error: ${err.message}`);
  });

  rtpReceiver.bind(RTP_PORT, '127.0.0.1'); // Bind to local port
}

// Convert a single μ-law sample to 16-bit PCM
function muLawToPcm16(muLaw) {
  muLaw = ~muLaw & 0xFF; // Invert bits and mask
  const sign = (muLaw & 0x80) ? -1 : 1; // Extract sign
  const exponent = (muLaw & 0x70) >> 4; // Extract exponent
  const mantissa = muLaw & 0x0F; // Extract mantissa
  let sample = (exponent === 0) ? (mantissa * 8 + 16) : (1 << (exponent + 3)) * (mantissa + 16) - 128; // Decode sample
  sample = sign * sample;
  return Math.max(-32768, Math.min(32767, sample)); // Clamp to 16-bit range
}

// Convert μ-law buffer to 24kHz PCM with interpolation
function muLawToPcm24kHz(muLawBuffer, channelId) {
  const pcm8kHz = Buffer.alloc(muLawBuffer.length * 2); // Buffer for 8kHz PCM
  let maxSampleBefore = 0; // Track max sample before clamping
  let maxSampleAfter = 0; // Track max sample after clamping

  // Convert μ-law to 8kHz PCM
  for (let i = 0; i < muLawBuffer.length; i++) {
    let sample = muLawToPcm16(muLawBuffer[i]);
    maxSampleBefore = Math.max(maxSampleBefore, Math.abs(sample));
    sample = Math.round(sample);
    sample = Math.max(-32768, Math.min(32767, sample));
    maxSampleAfter = Math.max(maxSampleAfter, Math.abs(sample));
    pcm8kHz.writeInt16LE(sample, i * 2);
  }

  // Upsample to 24kHz with linear interpolation
  const pcm24kHz = Buffer.alloc(muLawBuffer.length * 3 * 2);
  let sumSquares = 0;
  for (let i = 0; i < muLawBuffer.length; i++) {
    const sample = pcm8kHz.readInt16LE(i * 2);
    const prevSample = i > 0 ? pcm8kHz.readInt16LE((i - 1) * 2) : sample;
    const nextSample = i < muLawBuffer.length - 1 ? pcm8kHz.readInt16LE((i + 1) * 2) : sample;
    const interp1 = Math.round((prevSample * 0.5 + sample * 0.5)); // First interpolated sample
    const interp2 = Math.round((sample * 0.75 + nextSample * 0.25)); // Second interpolated sample
    pcm24kHz.writeInt16LE(prevSample, (i * 3) * 2);
    pcm24kHz.writeInt16LE(interp1, (i * 3 + 1) * 2);
    pcm24kHz.writeInt16LE(interp2, (i * 3 + 2) * 2);
    sumSquares += prevSample * prevSample + interp1 * interp1 + interp2 * interp2; // For RMS calculation
  }

  const rms = Math.sqrt(sumSquares / (muLawBuffer.length * 3)) / 32768; // Calculate RMS
  if (!audioToOpenAIMap.has(channelId)) audioToOpenAIMap.set(channelId, Buffer.alloc(0));
  audioToOpenAIMap.set(channelId, Buffer.concat([audioToOpenAIMap.get(channelId), pcm24kHz])); // Append to OpenAI buffer

  const now = Date.now();
  if (!amplificationLogFrequency.has(channelId) || now - amplificationLogFrequency.get(channelId) >= 2000) {
    logger.info(`Audio processed for channel ${channelId} | RMS: ${rms.toFixed(3)} | Max sample before: ${maxSampleBefore}, after: ${maxSampleAfter}`);
    amplificationLogFrequency.set(channelId, now); // Update log frequency
  }

  return pcm24kHz;
}

// Save PCM data as a WAV file
function saveWavFile(pcmData, filename, sampleRate) {
  const bitsPerSample = 16; // 16-bit audio
  const channels = 1; // Mono
  const byteRate = sampleRate * channels * (bitsPerSample / 8);
  const blockAlign = channels * (bitsPerSample / 8);
  const dataSize = pcmData.length;
  const fileSize = 36 + dataSize;

  const buffer = Buffer.alloc(44 + dataSize); // WAV header + data
  buffer.write('RIFF', 0);
  buffer.writeUInt32LE(fileSize, 4);
  buffer.write('WAVE', 8);
  buffer.write('fmt ', 12);
  buffer.writeUInt32LE(16, 16); // Subchunk size
  buffer.writeUInt16LE(1, 20); // PCM format
  buffer.writeUInt16LE(channels, 22);
  buffer.writeUInt32LE(sampleRate, 24);
  buffer.writeUInt32LE(byteRate, 28);
  buffer.writeUInt16LE(blockAlign, 32);
  buffer.writeUInt16LE(bitsPerSample, 34);
  buffer.write('data', 36);
  buffer.writeUInt32LE(dataSize, 40);
  pcmData.copy(buffer, 44); // Copy PCM data

  fs.writeFileSync(filename, buffer);
  logger.info(`Saved audio as ${filename}`);
}

// Save raw μ-law data to a file
function saveRawFile(data, filename) {
  fs.writeFileSync(filename, data);
  logger.info(`Saved raw μ-law as ${filename}`);
}

// Convert 16-bit PCM sample to μ-law
function pcm16ToMuLaw(sample) {
  const MAX = 32767;
  const MU = 255;
  const BIAS = 33;

  sample = Math.max(-MAX, Math.min(MAX, sample)); // Clamp to 16-bit range
  const sign = sample < 0 ? 0x80 : 0;
  let absSample = Math.abs(sample);

  if (absSample < 50) return 0x7F; // Silence threshold
  absSample += BIAS;

  const normalized = absSample / MAX;
  const muLaw = Math.log(1 + MU * normalized) / Math.log(1 + MU); // μ-law compression
  const quantized = Math.round(muLaw * 128);
  const exponent = Math.min(Math.floor(quantized / 16), 7);
  const mantissa = Math.min((quantized - (exponent * 16)), 15) & 0x0F;

  return ~(sign | (exponent << 4) | mantissa) & 0xFF; // Invert bits
}

// Resample 24kHz PCM to 8kHz
function resamplePcm24kHzTo8kHz(pcm24kHz) {
  const inSampleRate = 24000;
  const outSampleRate = 8000;
  const inSamples = pcm24kHz.length / 2;
  const outSamples = Math.floor(inSamples * outSampleRate / inSampleRate);
  const pcm8kHz = Buffer.alloc(outSamples * 2);

  for (let i = 0; i < outSamples; i++) {
    const srcPos = i * inSampleRate / outSampleRate;
    const srcIndex = Math.floor(srcPos);
    const frac = srcPos - srcIndex;

    if (srcIndex + 1 < inSamples) {
      const sample1 = pcm24kHz.readInt16LE(srcIndex * 2);
      const sample2 = pcm24kHz.readInt16LE((srcIndex + 1) * 2);
      const interpSample = Math.round(sample1 + frac * (sample2 - sample1)); // Linear interpolation
      pcm8kHz.writeInt16LE(interpSample, i * 2);
    } else if (srcIndex < inSamples) {
      pcm8kHz.writeInt16LE(pcm24kHz.readInt16LE(srcIndex * 2), i * 2);
    }
  }
  return pcm8kHz;
}

// Convert PCM buffer to μ-law, optionally resampling from 24kHz to 8kHz
function pcmToMuLaw(pcmBuffer, resample = false) {
  const input = resample ? resamplePcm24kHzTo8kHz(pcmBuffer) : pcmBuffer;
  const muLawBuffer = Buffer.alloc(input.length / 2);
  const chunkSize = 1024;
  for (let i = 0; i < input.length / 2; i += chunkSize) {
    const end = Math.min(i + chunkSize, input.length / 2);
    for (let j = i; j < end; j++) {
      let sample = input.readInt16LE(j * 2);
      sample = Math.max(-32767, Math.min(32767, Math.floor(sample * 0.95))); // Apply slight attenuation
      muLawBuffer[j] = pcm16ToMuLaw(sample);
    }
  }
  return muLawBuffer;
}

// Build RTP header for a packet
function buildRTPHeader(seq, timestamp, ssrc) {
  const header = Buffer.alloc(12);
  header[0] = 0x80; // Version 2, no padding, no extension
  header[1] = 0x00; // Payload type (0 for μ-law)
  header.writeUInt16BE(seq, 2); // Sequence number
  header.writeUInt32BE(timestamp, 4); // Timestamp
  header.writeUInt32BE(ssrc, 8); // Synchronization source
  return header;
}

// Async queue for sending RTP packets
const rtpQueue = async.queue((task, callback) => {
  rtpSender.send(task.packet, task.port, task.address, callback);
}, RTP_QUEUE_CONCURRENCY);

// Send an RTP packet with μ-law data
async function sendAudioPacket(muLawData, port, address, seq, timestamp, ssrc) {
  const startTime = process.hrtime.bigint();
  const header = buildRTPHeader(seq, timestamp, ssrc);
  const rtpPacket = Buffer.concat([header, muLawData]);
  await new Promise((resolve, reject) => {
    rtpQueue.push({ packet: rtpPacket, port, address }, (err) => {
      const elapsedMs = Number(process.hrtime.bigint() - startTime) / 1e6;
      if (ENABLE_RTP_LOGGING && seq % LOG_RTP_EVERY_N_PACKETS === 0) {
        logger.info(`Sent packet seq=${seq}, timestamp=${timestamp}, elapsed=${elapsedMs.toFixed(2)}ms`);
      }
      if (err) {
        logger.error(`Error sending RTP packet: ${err.message}`);
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

// Stream audio to Asterisk via RTP
const MAX_BUFFER_SIZE = 1024 * 1024; // Max buffer size (1MB)
async function streamAudio(channelId, rtpSource, initialBuffer = Buffer.alloc(0)) {
  const samplesPerPacket = 80; // 10 ms at 8000 Hz
  const packetIntervalNs = BigInt(10 * 1e6); // 10 ms in nanoseconds
  const { address, port } = rtpSource;

  logger.info(`Initializing RTP stream to ${address}:${port} for channel ${channelId}`);

  let rtpSequence = Math.floor(Math.random() * 65535); // Random initial sequence
  let rtpTimestamp = 0; // Initial timestamp
  const rtpSSRC = Math.floor(Math.random() * 4294967295); // Random SSRC
  let streamStartTime = process.hrtime.bigint();
  let isStreaming = true;
  let totalBytesSent = 0;
  let totalPacketsSent = 0;
  let stopRequested = false;
  let lastBufferSize = 0; // Previous buffer size
  let wasSending = false; // Track if we were sending data

  let muLawBuffer = Buffer.alloc(0); // Buffer for μ-law data
  let offset = 0; // Offset in buffer

  if (!rtpSentStats.has(channelId)) {
    rtpSentStats.set(channelId, { packets: 0, bytes: 0, startTime: null }); // Initialize stats
  }

  // Send a batch of RTP packets
  const sendPackets = async (data, packetCount, isSilence = false) => {
    let blockStartTime = process.hrtime.bigint();
    let nextPacketTime = blockStartTime;

    for (let i = 0; i < packetCount && !stopRequested; i++) {
      const bytesToSend = Math.min(samplesPerPacket, data.length - (i * samplesPerPacket));
      const packetData = data.slice(i * samplesPerPacket, i * samplesPerPacket + bytesToSend);
      const packetDataPadded = bytesToSend < samplesPerPacket ? Buffer.concat([packetData, Buffer.alloc(samplesPerPacket - bytesToSend, 0x7F)]) : packetData;

      await sendAudioPacket(packetDataPadded, port, address, rtpSequence, rtpTimestamp, rtpSSRC);
      if (i === 0 && !streamStartTime) streamStartTime = process.hrtime.bigint();
      rtpSequence = (rtpSequence + 1) % 65536;
      rtpTimestamp += 80;
      totalBytesSent += packetDataPadded.length;
      totalPacketsSent += 1;

      const stats = rtpSentStats.get(channelId);
      stats.packets += 1;
      stats.bytes += packetDataPadded.length;
      if (!stats.startTime) stats.startTime = Date.now();

      nextPacketTime += packetIntervalNs;
      const now = process.hrtime.bigint();
      if (now < nextPacketTime) {
        const delayMs = Number(nextPacketTime - now) / 1e6;
        await new Promise(resolve => setTimeout(resolve, delayMs)); // Maintain timing
      }
    }
  };

  const silencePacket = Buffer.alloc(samplesPerPacket, 0x7F); // Silence packet
  await sendPackets(silencePacket, 10, true); // Send initial silence
  logger.info(`RTP stream fully initialized for channel ${channelId}`);

  // Process PCM chunks into μ-law
  const processFallback = async (pcmChunk) => {
    const muLawData = pcmToMuLaw(pcmChunk, true);
    muLawBuffer = Buffer.concat([muLawBuffer, muLawData]);
    if (muLawBuffer.length > MAX_BUFFER_SIZE) {
      muLawBuffer = muLawBuffer.slice(muLawBuffer.length - MAX_BUFFER_SIZE); // Trim buffer
    }
  };

  // Main streaming loop
  const streamLoop = async () => {
    while (isStreaming && !stopRequested) {
      if (!sipMap.has(channelId)) {
        logger.info(`Channel ${channelId} no longer active, stopping RTP stream`);
        break;
      }
      const currentBufferSize = muLawBuffer.length - offset;
      if (currentBufferSize >= samplesPerPacket) {
        const packetCount = Math.floor(currentBufferSize / samplesPerPacket);
        await sendPackets(muLawBuffer.slice(offset, offset + packetCount * samplesPerPacket), packetCount);
        offset += packetCount * samplesPerPacket;
        if (muLawBuffer.length - offset > MAX_BUFFER_SIZE / 2) {
          muLawBuffer = muLawBuffer.slice(offset);
          offset = 0; // Reset offset
        }
        wasSending = true;
      } else if (wasSending && currentBufferSize < samplesPerPacket) {
        logger.info(`RTP buffer to Asterisk fully sent for channel ${channelId} | Remaining: ${currentBufferSize} bytes`);
        wasSending = false;
      }
      lastBufferSize = currentBufferSize;
      await new Promise(resolve => setImmediate(resolve)); // Yield control
    }

    const totalDuration = Number(process.hrtime.bigint() - streamStartTime) / 1e9;
    logger.info(`Finished RTP stream for channel ${channelId} | Total duration: ${totalDuration.toFixed(2)}s | Total bytes sent: ${totalBytesSent} | Total packets: ${totalPacketsSent}`);
    rtpSentStats.set(channelId, { packets: 0, bytes: 0, startTime: null }); // Reset stats
  };

  streamLoop();

  // Stop the stream
  const stop = async () => {
    isStreaming = false;
    stopRequested = true;
    muLawBuffer = Buffer.alloc(0);
    offset = 0;
    logger.info(`RTP stream stopped for channel ${channelId}`);
  };

  return {
    stop,
    write: processFallback, // Method to write PCM data
    muLawBuffer,
    offset
  };
}

// Start WebSocket connection to OpenAI real-time API
function startOpenAIWebSocket(channelId) {
  logger.info(`Attempting to start OpenAI WebSocket for channel ${channelId}`);
  const ws = new WebSocket(REALTIME_URL, {
    headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'OpenAI-Beta': 'realtime=v1' } // Authentication headers
  });

  let responseTimestamp = null; // Timestamp of response start
  let responseTranscript = ''; // Accumulated transcript
  let audioDeltaCount = 0; // Count of audio fragments
  let transcriptDeltaCount = 0; // Count of transcript fragments
  let audioReceivedLogged = false; // Flag for first audio log
  let audioSentTime = null; // Time audio was sent to OpenAI
  let callStartTime = null; // Call start time
  let maxCallTimeoutId = null; // Timeout ID for max call duration
  let totalPacketsSentThisResponse = 0; // Packets sent for current response
  let totalPacketsSentSession = 0; // Total packets sent in session
  let playbackComplete = false; // Playback completion flag
  let streamHandler = null; // RTP stream handler
  let isPlayingResponse = false; // Flag for active response playback

  // Initialize RTP stream handler
  const initializeStreamHandler = async () => {
    const channelData = sipMap.get(channelId);
    if (channelData && channelData.rtpSource) {
      streamHandler = await streamAudio(channelId, channelData.rtpSource);
      logger.info(`StreamHandler initialized for channel ${channelId} | Ready: ${streamHandler !== null}`);
    } else {
      logger.error(`Cannot initialize StreamHandler: No RTP source for channel ${channelId}`);
    }
    return streamHandler;
  };

  // WebSocket open event
  ws.on('open', async () => {
    callStartTime = Date.now();
    logClient(`OpenAI WebSocket connection established for channel ${channelId}`);
    await initializeStreamHandler();
    ws.send(JSON.stringify({
      type: 'session.update',
      session: {
        modalities: ['audio', 'text'], // Enable audio and text responses
        voice: 'alloy', // Voice for OpenAI responses
        instructions: 'Always respond with audio to any detected speech.',
        turn_detection: {
          type: 'server_vad', // Server-side Voice Activity Detection
          threshold: VAD_THRESHOLD,
          prefix_padding_ms: VAD_PREFIX_PADDING_MS,
          silence_duration_ms: VAD_SILENCE_DURATION_MS
        },
        input_audio_transcription: { model: 'whisper-1' } // Transcription model
      }
    }));
    logClient(`Session updated with VAD settings for channel ${channelId} | Threshold: ${VAD_THRESHOLD}, Prefix: ${VAD_PREFIX_PADDING_MS}ms, Silence: ${VAD_SILENCE_DURATION_MS}ms`);

    // Set max call duration timeout
    maxCallTimeoutId = setTimeout(async () => {
      logClient(`Max call duration (${MAX_CALL_DURATION}ms) reached for channel ${channelId}, closing connection and hanging up`);
      ws.close();
      const channelData = sipMap.get(channelId);
      if (channelData && channelData.bridge) {
        try {
          await ariClient.channels.hangup({ channelId: channelId });
          logger.info(`Channel ${channelId} hung up due to max call duration`);
        } catch (err) {
          logger.error(`Failed to hang up channel ${channelId}: ${err.message}`);
        }
      }
    }, MAX_CALL_DURATION);
  });

  // Handle incoming WebSocket messages from OpenAI
  ws.on('message', async (data) => {
    const response = JSON.parse(data.toString());
    receivedEventCounter++;
    const duration = audioSentTime ? ((Date.now() - audioSentTime) / 1000).toFixed(2) : 'N/A';

    if (receivedEventCounter === 0) {
      logServer(`First event received for channel ${channelId} | Type: ${response.type} | Duration: ${duration}s | Status: Received`);
    }

    switch (response.type) {
      case 'session.created':
        logServer(`Session created for channel ${channelId} | Duration: ${duration}s | Status: Received`);
        break;
      case 'input_audio_buffer.speech_started':
        logServer(`Speech started detected for channel ${channelId} | Duration: ${duration}s | Status: Received`);
        break;
      case 'input_audio_buffer.speech_stopped':
        logServer(`Speech stopped detected for channel ${channelId} | Duration: ${duration}s | Status: Received`);
        audioSentTime = Date.now();
        if (streamHandler) {
          await streamHandler.stop();
          logger.info(`Stopped RTP stream due to user speech for channel ${channelId}`);
          streamHandler = null;
          await initializeStreamHandler();
          isPlayingResponse = false;
        }
        break;
      case 'conversation.item.input_audio_transcription.completed':
        logServer(`Sent audio transcribed for channel ${channelId} | Transcript: "${response.transcript.trim()}" | Duration: ${duration}s | Status: Received`);
        break;
      case 'response.audio.delta':
        audioDeltaCount++;
        if (!audioReceivedLogged) {
          responseTimestamp = Date.now();
          logServer(`Audio reception started for channel ${channelId} | Duration: ${duration}s | Status: Received`);
          audioReceivedLogged = true;
        }
        isPlayingResponse = true;
        const pcmChunk = Buffer.from(response.delta, 'base64'); // Decode audio chunk
        logServer(`Audio delta received for channel ${channelId} | Size: ${(pcmChunk.length / 1024).toFixed(2)} KB`);
        if (streamHandler) {
          streamHandler.write(pcmChunk); // Send to Asterisk
          totalPacketsSentThisResponse += pcmChunk.length / 160;
          totalPacketsSentSession += pcmChunk.length / 160;
        } else {
          logger.error(`Failed to write audio delta: No StreamHandler for channel ${channelId}`);
        }
        break;
      case 'response.audio_transcript.delta':
        transcriptDeltaCount++;
        responseTranscript += response.delta; // Accumulate transcript
        break;
      case 'response.audio_transcript.done':
        logServer(`Response received for channel ${channelId} | Transcript: "${response.transcript.trim()}" | Duration: ${duration}s | Status: Received`);
        responseTranscript = '';
        break;
      case 'response.done':
        audioReceivedLogged = false;
        isPlayingResponse = false;
        const stats = rtpSentStats.get(channelId) || { packets: 0, bytes: 0, startTime: responseTimestamp };
        const responseDuration = responseTimestamp ? ((Date.now() - responseTimestamp) / 1000).toFixed(2) : 'N/A';
        logServer(`Response completed for channel ${channelId} | Duration: ${responseDuration}s | Audio Fragments: ${audioDeltaCount} | Text Fragments: ${transcriptDeltaCount} | RTP Packets: ${stats.packets} | RTP Bytes: ${stats.bytes}`);
        audioDeltaCount = 0;
        transcriptDeltaCount = 0;
        totalPacketsSentThisResponse = 0;
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'input_audio_buffer.clear' })); // Clear OpenAI buffer
          logClient(`Cleared OpenAI audio buffer for channel ${channelId}`);
        }
        break;
      case 'error':
        logServer(`Error received for channel ${channelId} | Message: ${response.error.message} | Code: ${response.error.code || 'N/A'} | Status: Error`);
        if (streamHandler) {
          await streamHandler.stop();
          streamHandler = null;
        }
        break;
    }
  });

  ws.on('error', (error) => {
    logClient(`OpenAI WebSocket error for channel ${channelId} | Message: ${error.message} | Status: Error`);
    if (streamHandler) {
      streamHandler.stop();
      streamHandler = null;
    }
  });

  ws.on('close', () => {
    if (maxCallTimeoutId) clearTimeout(maxCallTimeoutId);
    if (streamHandler) {
      streamHandler.stop();
      streamHandler = null;
    }
    logClient(`OpenAI WebSocket connection closed for channel ${channelId} | Status: Finished`);
  });

  return { ws, getPlaybackComplete: () => playbackComplete, stopStream: () => streamHandler && streamHandler.stop() };
}

// Main async function to initialize ARI and handle events
(async () => {
  try {
    ariClient = await ari.connect(ARI_URL, ARI_USER, ARI_PASS); // Connect to ARI
    logger.info(`Connected to ARI at ${ARI_URL}`);
    await ariClient.start(ARI_APP); // Start Stasis app
    logger.info(`ARI application "${ARI_APP}" started`);

    startRTPReceiver(); // Start RTP receiver

    // Handle new channel entering Stasis
    ariClient.on('StasisStart', async (evt, channel) => {
      logger.info(`StasisStart event received for channel ${channel.id}, name: ${channel.name}`);
      if (channel.name && channel.name.startsWith('UnicastRTP')) { // ExternalMedia channel
        logger.info(`ExternalMedia channel started: ${channel.id}`);
        let mapping = extMap.get(channel.id);
        if (!mapping) {
          await new Promise(r => setTimeout(r, 500)); // Wait for mapping
          mapping = extMap.get(channel.id);
        }
        if (mapping) {
          await addExtToBridge(ariClient, channel, mapping.bridgeId);
          const channelData = sipMap.get(mapping.channelId);
          if (channelData && !channelData.rtpSource) {
            rtpReceiver.once('message', (msg, rinfo) => {
              channelData.rtpSource = rinfo; // Assign RTP source
              logger.info(`RTP Source assigned for channel ${mapping.channelId}: ${rinfo.address}:${rinfo.port}`);
            });
          }
        }
        return;
      }
      logger.info(`SIP channel started: ${channel.id}`);
      try {
        const bridge = await ariClient.bridges.create({ type: 'mixing,proxy_media' }); // Create mixing bridge
        await bridge.addChannel({ channel: channel.id });

        await channel.answer(); // Answer the call
        logger.info(`Channel ${channel.id} answered`);

        // Set up ExternalMedia channel
        const extParams = {
          app: ARI_APP,
          external_host: `127.0.0.1:${RTP_PORT}`,
          format: 'ulaw',
          transport: 'udp',
          encapsulation: 'rtp',
          connection_type: 'client',
          direction: 'both'
        };
        const extChannel = await ariClient.channels.externalMedia(extParams);
        extMap.set(extChannel.id, { bridgeId: bridge.id, channelId: channel.id });
        logger.info(`ExternalMedia channel ${extChannel.id} created and mapped to bridge ${bridge.id}`);

        const { ws, getPlaybackComplete, stopStream } = startOpenAIWebSocket(channel.id);
        sipMap.set(channel.id, { bridge, ws, channelId: channel.id, sendTimeout: null, getPlaybackComplete, stopStream });
      } catch (e) {
        logger.error(`Error in SIP channel ${channel.id}: ${e.message}`);
      }
    });

    // Handle channel leaving Stasis (call end)
    ariClient.on('StasisEnd', async (evt, channel) => {
      if (channel.name && channel.name.startsWith('UnicastRTP')) {
        extMap.delete(channel.id);
        logger.info(`ExternalMedia channel ${channel.id} removed from map`);
      } else {
        const channelData = sipMap.get(channel.id);
        if (channelData) {
          try {
            sipMap.delete(channel.id);
            logger.info(`Channel ${channel.id} removed from sipMap at start of StasisEnd`);

            if (channelData.sendTimeout) {
              clearInterval(channelData.sendTimeout);
              channelData.sendTimeout = null;
              logger.info(`Send timeout cleared for channel ${channel.id}`);
            }

            if (channelData.stopStream) {
              await channelData.stopStream();
              logger.info(`StreamHandler stopped for channel ${channel.id} in StasisEnd`);
            }

            if (!channelData.getPlaybackComplete()) {
              logger.info(`Channel ${channel.id} hung up, checking playback status before cleanup`);
              await new Promise(resolve => setTimeout(resolve, 100)); // Brief delay
            }

            if (channelData.ws && channelData.ws.readyState === WebSocket.OPEN) {
              channelData.ws.close();
              logger.info(`WebSocket closed for channel ${channel.id} in StasisEnd`);
            }

            await channelData.bridge.destroy();
            logger.info(`Bridge ${channelData.bridge.id} destroyed`);
          } catch (e) {
            logger.error(`Error during cleanup for channel ${channel.id}: ${e.message}`);
          }
        }
        logger.info(`Channel ended: ${channel.id}`);

        // Save audio files if enabled
        if (ENABLE_SENT_TO_OPENAI_RECORDING && audioFromAsteriskMap.has(channel.id) && audioFromAsteriskMap.get(channel.id).length > 0) {
          saveRawFile(audioFromAsteriskMap.get(channel.id), `asterisk_input_mulaw_raw_${channel.id}.raw`);
          audioFromAsteriskMap.delete(channel.id);
        }
        if (ENABLE_SENT_TO_OPENAI_RECORDING && audioToOpenAIMap.has(channel.id) && audioToOpenAIMap.get(channel.id).length > 0) {
          saveWavFile(audioToOpenAIMap.get(channel.id), `sent_to_openai_${channel.id}.wav`, 24000);
          audioToOpenAIMap.delete(channel.id);
        }
      }
    });

    ariClient.on('error', (err) => logger.error(`ARI client error: ${err.message}`));
    ariClient.on('close', () => logger.info('ARI WebSocket connection closed'));
  } catch (err) {
    logger.error(`ARI connection error: ${err.message}`);
    process.exit(1); // Exit on connection failure
  }
})();

// Handle uncaught exceptions
process.on('uncaughtException', (err) => {
  logger.error(`Uncaught Exception: ${err.message}`);
  cleanup();
  process.exit(1);
});

// Handle SIGINT (Ctrl+C)
process.on('SIGINT', () => {
  logger.info('Received SIGINT, cleaning up...');
  cleanup();
  process.exit(0);
});

// Cleanup function to close sockets and connections
function cleanup() {
  sipMap.forEach((data, channelId) => {
    if (data.ws) data.ws.close(); // Close WebSocket
    if (data.sendTimeout) clearInterval(data.sendTimeout); // Clear send interval
    if (data.stopStream) data.stopStream(); // Stop RTP stream
  });
  rtpSender.close(); // Close RTP sender socket
  rtpReceiver.close(); // Close RTP receiver socket
}
