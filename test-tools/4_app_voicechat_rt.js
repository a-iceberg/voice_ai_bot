// 4_app_voicechat_rt.js
const WebSocket = require('ws');
const fs = require('fs');
require('dotenv').config();

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const REALTIME_API_URL = 'wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17';
const inputAudioPath = './input_audio.pcm';

const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
const outputAudioPath = `./output_audio_${timestamp}.pcm`;

const getTimestamp = () => new Date().toISOString();

async function sendAudioInChunks(ws, audioData) {
    const chunkSize = 4800; // 100 ms of audio at 24kHz, 16-bit mono
    const delayMs = 100;    // Delay to simulate real-time streaming

    for (let i = 0; i < audioData.length; i += chunkSize) {
        const chunk = audioData.slice(i, Math.min(i + chunkSize, audioData.length));
        const audioBase64 = chunk.toString('base64');
        ws.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: audioBase64 }));
        console.log(`${getTimestamp()} - Sent chunk: ${chunk.length} bytes`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
    }
}

async function sendAudioAndGetAudioResponse() {
    console.log(`${getTimestamp()} - Starting connection...`);
    fs.writeFileSync(outputAudioPath, Buffer.alloc(0));
    console.log(`${getTimestamp()} - Output file initialized: ${outputAudioPath}`);

    const ws = new WebSocket(REALTIME_API_URL, {
        headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'OpenAI-Beta': 'realtime=v1' }
    });

    ws.on('open', () => {
        console.log(`${getTimestamp()} - Connection established`);
        // VAD is enabled by default, no need to modify

        const audioData = fs.readFileSync(inputAudioPath);
        console.log(`${getTimestamp()} - Audio loaded, total size: ${audioData.length} bytes`);

        sendAudioInChunks(ws, audioData).then(() => {
            ws.send(JSON.stringify({ type: 'input_audio_buffer.commit' }));
            console.log(`${getTimestamp()} - Audio confirmed`);

            ws.send(JSON.stringify({
                type: 'response.create',
                response: {
                    modalities: ['audio', 'text'],
                    output_audio_format: 'pcm16',
                    instructions: 'Respond in English with a clear and friendly voice to the received audio. If you don\'t understand the audio, say "I didn\'t understand the audio, can you repeat it?" with a clear voice.'
                }
            }));
            console.log(`${getTimestamp()} - Response request sent`);
        });
    });

    let transcriptBuffer = '';

    ws.on('message', (data) => {
        try {
            const response = JSON.parse(data.toString());
            switch (response.type) {
                case 'error':
                    console.log(`${getTimestamp()} - Error: ${response.error.message}`);
                    break;
                case 'response.text.done':
                    console.log(`${getTimestamp()} - Response in text: ${response.text}`);
                    break;
                case 'response.audio_transcript.delta':
                    transcriptBuffer += response.delta;
                    console.log(`${getTimestamp()} - Partial transcription: ${transcriptBuffer}`);
                    break;
                case 'response.audio_transcript.done':
                    console.log(`${getTimestamp()} - Complete transcription: ${transcriptBuffer}`);
                    break;
                case 'response.audio.delta':
                    if (!response.delta || typeof response.delta !== 'string') {
                        console.log(`${getTimestamp()} - Error: Invalid or empty audio delta`);
                        return;
                    }
                    const audioBuffer = Buffer.from(response.delta, 'base64');
                    fs.appendFileSync(outputAudioPath, audioBuffer);
                    console.log(`${getTimestamp()} - Audio received: ${audioBuffer.length} bytes`);
                    break;
                case 'response.done':
                    console.log(`${getTimestamp()} - Audio saved to: ${outputAudioPath}`);
                    ws.close();
                    break;
                case 'input_audio_buffer.speech_started':
                    console.log(`${getTimestamp()} - Voice detection started`);
                    break;
                case 'conversation.item.created':
                    if (response.item?.content?.[0]?.type === 'input_audio') {
                        console.log(`${getTimestamp()} - Audio transcribed (optional): ${response.item.content[0].transcript || 'No transcription'}`);
                    }
                    break;
            }
        } catch (error) {
            console.error(`${getTimestamp()} - Error processing message: ${error.message}`);
        }
    });

    ws.on('error', (error) => console.error(`${getTimestamp()} - Error in WebSocket: ${error.message}`));
    ws.on('close', () => console.log(`${getTimestamp()} - Connection closed`));
}

sendAudioAndGetAudioResponse().catch(error => console.error(`${getTimestamp()} - Error in execution: ${error.message}`));
