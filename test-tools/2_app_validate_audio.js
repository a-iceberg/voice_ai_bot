// 2_app_validate_audio.js
const { OpenAI } = require('openai');
const fs = require('fs');
require('dotenv').config();

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const inputAudioPath = './input_audio.pcm';

const getTimestamp = () => new Date().toISOString();

async function validateStandardSTT() {
    const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

    console.log(`${getTimestamp()} - Starting standard transcription...`);

    try {
        // Convert PCM to WAV temporarily for the standard API
        const wavPath = './input_audio.wav';
        require('child_process').execSync(`ffmpeg -f s16le -ar 24000 -ac 1 -i ${inputAudioPath} ${wavPath} -y`);

        const audioFile = fs.createReadStream(wavPath);
        const response = await openai.audio.transcriptions.create({
            model: 'whisper-1',
            file: audioFile,
            language: 'en' // Specify Spanish
        });

        console.log(`${getTimestamp()} - Audio transcribed: "${response.text}"`);
    } catch (error) {
        console.error(`${getTimestamp()} - Transcription error:`, error.message);
    }
}

validateStandardSTT().catch(error => {
    console.error(`${getTimestamp()} - Execution error:`, error.message);
});

process.on('SIGINT', () => {
    console.log(`${getTimestamp()} - Closing application...`);
    process.exit();
});
