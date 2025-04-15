// rtpServer.js

// Import required Node.js modules for UDP, file system, and WAV file handling
const dgram = require('dgram');  // Module for UDP socket communication (RTP)
const fs = require('fs');        // Module for file system operations (writing WAV files)
const wav = require('wav');     // Module for creating WAV files from PCM audio data

// RTPServer class to handle RTP audio recording and playback
class RTPServer {
  // Constructor with options for port, output file, and log interval
  constructor(options = {}) {
    this.port = options.port;                    // Port number where the RTP server listens
    this.outFile = options.outFile || 'call_recording.wav'; // Output WAV file name, defaults to 'call_recording.wav'
    this.server = dgram.createSocket('udp4');    // UDP socket created for RTP communication (IPv4)
    this.isRunning = false;                      // Flag indicating if the server is currently running
    this.wavWriter = null;                       // WAV writer instance, initialized later for audio recording
    this.packetCount = 0;                        // Counter for the number of RTP packets received
    this.logInterval = options.logInterval || 100; // Interval for logging packet summaries, defaults to every 100 packets
    this.lastRtpSource = null;                   // Stores the last RTP source address and port for playback
  }

  // Convert a single μ-law sample to linear PCM (16-bit)
  muLawToLinear(mu) {
    mu = ~mu & 0xFF;                           // Bitwise NOT and mask to get 8-bit μ-law value
    const sign = (mu & 0x80) ? -1 : 1;         // Extract sign bit (0x80): -1 for negative, 1 for positive
    const exponent = (mu >> 4) & 0x07;         // Extract 3-bit exponent (bits 4-6)
    const mantissa = mu & 0x0F;                // Extract 4-bit mantissa (bits 0-3)
    const sample = sign * (((mantissa << 1) + 33) << exponent) - 33; // Convert to linear PCM using μ-law formula
    return sample;                             // Return the 16-bit PCM sample
  }

  // Convert a buffer of μ-law samples to 16-bit PCM
  convertBuffer(muBuffer) {
    const numSamples = muBuffer.length;        // Number of μ-law samples in the buffer
    const pcmBuffer = Buffer.alloc(numSamples * 2); // Allocate buffer for PCM (2 bytes per sample)
    for (let i = 0; i < numSamples; i++) {     // Loop through each μ-law sample
      const mu = muBuffer[i];                  // Get the current μ-law sample
      const linear = this.muLawToLinear(mu);   // Convert it to linear PCM
      pcmBuffer.writeInt16LE(linear, i * 2);   // Write PCM sample as 16-bit little-endian
    }
    return pcmBuffer;                          // Return the PCM buffer
  }

  // Convert a single linear PCM (16-bit) sample to μ-law
  linearToMuLaw(sample) {
    const MULAW_MAX = 0x1FFF;                  // Maximum value for μ-law encoding (8191)
    const MULAW_BIAS = 33;                     // Bias value added during μ-law conversion
    let sign = (sample < 0) ? 0x80 : 0;        // Determine sign: 0x80 for negative, 0 for positive
    if (sample < 0) sample = -sample;          // Make sample positive if negative
    if (sample > MULAW_MAX) sample = MULAW_MAX; // Cap sample at maximum value
    sample += MULAW_BIAS;                      // Add bias to the sample
    let exponent = 7;                          // Start with maximum exponent (7)
    for (let expMask = 0x4000; (sample & expMask) === 0 && exponent > 0; exponent--, expMask >>= 1) {} // Find highest exponent
    let mantissa = (sample >> ((exponent === 0) ? 4 : (exponent + 3))) & 0x0F; // Extract 4-bit mantissa
    let muLawByte = ~(sign | (exponent << 4) | mantissa) & 0xFF; // Combine sign, exponent, mantissa and invert
    return muLawByte;                          // Return the 8-bit μ-law value
  }

  // Convert a buffer of 16-bit PCM samples to μ-law
  convertPCMBufferToMuLaw(pcmBuffer) {
    const numSamples = pcmBuffer.length / 2;   // Number of PCM samples (2 bytes per sample)
    const muLawBuffer = Buffer.alloc(numSamples); // Allocate buffer for μ-law samples (1 byte per sample)
    for (let i = 0; i < numSamples; i++) {     // Loop through each PCM sample
      const sample = pcmBuffer.readInt16LE(i * 2); // Read 16-bit PCM sample as little-endian
      muLawBuffer[i] = this.linearToMuLaw(sample); // Convert to μ-law and store
    }
    return muLawBuffer;                        // Return the μ-law buffer
  }

  // Build a simple 12-byte RTP header (Version 2, no padding, PT=0 PCMU)
  buildRTPHeader(seq, timestamp, ssrc) {
    const header = Buffer.alloc(12);           // Allocate 12 bytes for RTP header
    header[0] = 0x80;                          // Set RTP version 2 (first 2 bits = 10)
    header[1] = 0x00;                          // Payload Type 0 (PCMU, μ-law)
    header.writeUInt16BE(seq, 2);              // Write 16-bit sequence number (big-endian)
    header.writeUInt32BE(timestamp, 4);        // Write 32-bit timestamp (big-endian)
    header.writeUInt32BE(ssrc, 8);             // Write 32-bit SSRC identifier (big-endian)
    return header;                             // Return the completed RTP header
  }

  // Start the RTP server and begin recording audio to a WAV file
  start() {
    this.fileStream = fs.createWriteStream(this.outFile); // Create a write stream for the output WAV file
    this.wavWriter = new wav.Writer({          // Initialize WAV writer with specified audio parameters
      channels: 1,                             // Mono audio (1 channel)
      sampleRate: 8000,                        // Sample rate of 8 kHz
      bitDepth: 16                             // 16-bit PCM audio
    });
    this.wavWriter.pipe(this.fileStream);      // Pipe WAV writer output to the file stream

    this.server.on('listening', () => {        // Event handler for when the server starts listening
      const address = this.server.address();   // Get the server's listening address and port
      console.log(`[${new Date().toISOString()}] INFO: RTP Server listening on ${address.address}:${address.port}`); // Log server start
      this.isRunning = true;                   // Set running flag to true
    });

    this.server.on('message', (msg, rinfo) => { // Event handler for incoming RTP packets
      this.packetCount++;                      // Increment packet counter
      this.lastRtpSource = rinfo;              // Store source address and port of the last packet

      // Log the first RTP packet to confirm incoming flow
      if (this.packetCount === 1) {
        console.log(`[${new Date().toISOString()}] INFO: First RTP packet received from ${rinfo.address}:${rinfo.port}, size: ${msg.length}`); // Log first packet details
      }
      // Log summary every 100 packets for ongoing validation
      if (this.packetCount % this.logInterval === 0) {
        console.log(`[${new Date().toISOString()}] INFO: ${this.packetCount} RTP packets received`); // Log packet count summary
      }

      if (msg.length < 12) {                   // Check if packet is too short to have a valid RTP header
        console.log(`[${new Date().toISOString()}] ERROR: Packet too short (${msg.length} bytes)`); // Log error for invalid packet
        return;                                // Skip processing
      }
      const muPayload = msg.slice(12);         // Extract μ-law payload by skipping 12-byte RTP header
      const pcmBuffer = this.convertBuffer(muPayload); // Convert μ-law payload to PCM
      this.wavWriter.write(pcmBuffer);         // Write PCM data to the WAV file
    });

    this.server.on('error', (err) => {         // Event handler for server errors
      console.error(`[${new Date().toISOString()}] ERROR: RTP Server error: ${err.message}`); // Log error message
      this.stop();                             // Stop the server on error
    });

    try {
      this.server.bind({ address: '127.0.0.1', port: this.port }); // Bind the UDP socket to localhost and specified port
    } catch (e) {
      console.error(`[${new Date().toISOString()}] ERROR: Failed to bind RTP server to 127.0.0.1:${this.port}: ${e.message}`); // Log binding failure
    }
  }

  // Stop the RTP server and close the WAV file, logging final packet count
  stop() {
    if (this.isRunning) {                      // Check if the server is running
      console.log(`[${new Date().toISOString()}] INFO: Total RTP packets received: ${this.packetCount}`); // Log total packets received
      this.server.close(() => {                // Close the UDP socket
        console.log(`[${new Date().toISOString()}] INFO: RTP Server stopped on port ${this.port}`); // Log server stop
      });
      this.wavWriter.end(() => {               // Finalize the WAV file
        console.log(`[${new Date().toISOString()}] INFO: WAV file closed: ${this.outFile}`); // Log file closure
      });
      this.isRunning = false;                  // Set running flag to false
    }
  }

  // Playback the recorded WAV file via RTP
  playback() {
    if (!this.lastRtpSource) {                 // Check if there's a valid RTP source for playback
      console.error(`[${new Date().toISOString()}] ERROR: Unknown RTP source for playback`); // Log error if source is missing
      return;                                  // Exit if no source
    }

    fs.readFile(this.outFile, (err, data) => { // Read the recorded WAV file
      if (err) {                               // Check for file read errors
        console.error(`[${new Date().toISOString()}] ERROR: Failed to read WAV file: ${err.message}`); // Log error
        return;                                // Exit on error
      }
      const headerSize = 44;                   // Standard WAV header size (44 bytes)
      if (data.length <= headerSize) {         // Check if file is too short (only header)
        console.error(`[${new Date().toISOString()}] ERROR: WAV file too short`); // Log error
        return;                                // Exit if file is invalid
      }
      const pcmData = data.slice(headerSize);  // Extract PCM audio data, skipping header
      const muLawData = this.convertPCMBufferToMuLaw(pcmData); // Convert PCM to μ-law

      const packetSize = 160;                  // Size of each RTP packet (20ms at 8kHz, 160 bytes)
      const totalPackets = Math.ceil(muLawData.length / packetSize); // Calculate total packets to send

      let sequence = Math.floor(Math.random() * 65535); // Random initial sequence number (0-65535)
      let timestamp = Math.floor(Math.random() * 4294967295); // Random initial timestamp (32-bit)
      const ssrc = Math.floor(Math.random() * 4294967295); // Random SSRC identifier (32-bit)

      const sender = dgram.createSocket('udp4'); // Create UDP socket for sending RTP packets

      console.log(`[${new Date().toISOString()}] INFO: Starting RTP playback to ${this.lastRtpSource.address}:${this.lastRtpSource.port}. Total packets: ${totalPackets}`); // Log playback start

      let packetIndex = 0;                     // Index to track current packet
      const interval = setInterval(() => {     // Set interval to send packets every 20ms
        if (packetIndex >= totalPackets) {     // Check if all packets have been sent
          clearInterval(interval);             // Stop the interval
          sender.close();                      // Close the sender socket
          console.log(`[${new Date().toISOString()}] INFO: RTP playback finished`); // Log playback completion
          return;                              // Exit the interval function
        }
        const startByte = packetIndex * packetSize; // Calculate start byte for current packet
        const endByte = Math.min(startByte + packetSize, muLawData.length); // Calculate end byte, capped at buffer length
        const payload = muLawData.slice(startByte, endByte); // Extract payload for current packet

        const header = this.buildRTPHeader(sequence, timestamp, ssrc); // Build RTP header for the packet
        const rtpPacket = Buffer.concat([header, payload]); // Combine header and payload into full RTP packet

        sender.send(rtpPacket, 0, rtpPacket.length, this.lastRtpSource.port, this.lastRtpSource.address, (err) => { // Send RTP packet
          if (err) console.error(`[${new Date().toISOString()}] ERROR: RTP send error: ${err.message}`); // Log send error
        });

        sequence = (sequence + 1) % 65536;     // Increment sequence number, wrap at 65536
        timestamp = (timestamp + 160) >>> 0;   // Increment timestamp by 160 (20ms), unsigned right shift to keep 32-bit
        packetIndex++;                         // Increment packet index
      }, 20); // Send every 20ms (matches 8kHz sample rate with 160 samples per packet)
    });
  }
}

// Export the RTPServer class for use in other modules (e.g., ari_app_with_rtp.js)
module.exports = RTPServer;
