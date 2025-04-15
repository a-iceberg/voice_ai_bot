// 3_app_textchat_rt.js
const WebSocket = require('ws');
require('dotenv').config();

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const REALTIME_API_URL = 'wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-12-17';

const getTimestamp = () => new Date().toISOString();
const prettyLog = (label, data) => {
    console.log(`${getTimestamp()} - ${label}:`, JSON.stringify(data, null, 2));
};

async function connectToRealtimeAPI() {
    console.log(`${getTimestamp()} - Starting connection...`);
    console.log(`${getTimestamp()} - Using URL: ${REALTIME_API_URL}`);
    console.log(`${getTimestamp()} - API Key (first 5 characters): ${OPENAI_API_KEY?.substring(0, 5)}...`);

    const ws = new WebSocket(REALTIME_API_URL, {
        headers: {
            Authorization: `Bearer ${OPENAI_API_KEY}`,
            'OpenAI-Beta': 'realtime=v1'
        }
    });

    ws.on('open', () => {
        prettyLog('Connection established with OpenAI Realtime API', { readyState: ws.readyState });

        const createItemMessage = {
            type: 'conversation.item.create',
            item: {
                type: 'message',
                role: 'user',
                content: [{ type: 'input_text', text: 'Hello, how are you today?' }]
            }
        };
        ws.send(JSON.stringify(createItemMessage));
        prettyLog('User conversation item created', createItemMessage);

        const responseMessage = {
            type: 'response.create',
            response: { modalities: ['text'] }
        };
        ws.send(JSON.stringify(responseMessage));
        prettyLog('Response request sent', responseMessage);
    });

    ws.on('message', (data) => {
        try {
            const response = JSON.parse(data);
            prettyLog('Message received', { type: response.type, fullResponse: response });
            if (response.type === 'response.text.done') {
                console.log(`${getTimestamp()} - Final response:`, response.text);
            }
        } catch (error) {
            prettyLog('Error parsing message', { rawData: data.toString(), error: error.message });
        }
    });

    ws.on('error', (error) => {
        prettyLog('WebSocket error', { message: error.message, code: error.code, stack: error.stack });
    });

    ws.on('close', (code, reason) => {
        prettyLog('Connection closed', { code: code, reason: reason.toString() });
    });
}

connectToRealtimeAPI().catch(error => {
    prettyLog('Error starting connection', { message: error.message, stack: error.stack });
});

process.on('SIGINT', () => {
    console.log(`${getTimestamp()} - Closing application...`);
    process.exit();
});
