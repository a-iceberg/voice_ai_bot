// 1_app_tts.js
const { OpenAI } = require('openai');
const fs = require('fs');
const { execSync } = require('child_process');
require('dotenv').config();

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const ttsOutputWav = './tts_output.wav';      // Initial output in WAV
const ttsOutputPcm = './input_audio.pcm';     // Converted output to PCM16

const getTimestamp = () => new Date().toISOString();

async function generateTTSAndConvert() {
    const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
    const textToSpeak = "Hello, what AI model are you?";

    console.log(`${getTimestamp()} - Generating TTS for: "${textToSpeak}"`);
    try {
        // Generate TTS audio in WAV
        const response = await openai.audio.speech.create({
            model: 'tts-1',
            voice: 'alloy',
            input: textToSpeak,
            response_format: 'wav'  // Changed to WAV
        });

        const buffer = Buffer.from(await response.arrayBuffer());
        fs.writeFileSync(ttsOutputWav, buffer);
        console.log(`${getTimestamp()} - TTS audio saved to: ${ttsOutputWav}`);

        // Convert WAV to PCM16 (24kHz, mono) with standard volume
        console.log(`${getTimestamp()} - Converting WAV to PCM16 with standard volume...`);
        execSync(`ffmpeg -i ${ttsOutputWav} -f s16le -acodec pcm_s16le -ar 24000 -ac 1 ${ttsOutputPcm} -y`);
        console.log(`${getTimestamp()} - Audio converted to PCM16 saved to: ${ttsOutputPcm}`);
    } catch (error) {
        console.error(`${getTimestamp()} - Error:`, error.message);
    }
}

generateTTSAndConvert().catch(error => {
    console.error(`${getTimestamp()} - General error:`, error.message);
});

process.on('SIGINT', () => {
    console.log(`${getTimestamp()} - Closing application...`);
    process.exit();
});
