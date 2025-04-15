// 0_app_check_key_realtime.js
require('dotenv').config();
const WebSocket = require('ws');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const REALTIME_API_URL = 'wss://api.openai.com/v1/realtime?model=gpt-4o';

async function checkRealtimeAccess() {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(REALTIME_API_URL, {
            headers: {
                Authorization: `Bearer ${OPENAI_API_KEY}`,
                'OpenAI-Beta': 'realtime=v1'
            }
        });

        ws.on('open', () => {
            console.log('Connection established with OpenAI Realtime API');
        });

        ws.on('message', (data) => {
            const response = JSON.parse(data);
            if (response.type === 'session.created') {
                console.log('Session created successfully. You have access to the Realtime API with the gpt-4o model.');
                resolve(true);
                ws.close();
            } else if (response.type === 'error') {
                console.log('Error in the Realtime API:', response.error.message);
                reject(new Error(response.error.message));
                ws.close();
            }
        });

        ws.on('error', (error) => {
            console.log('Connection error:', error.message);
            reject(error);
        });

        ws.on('close', () => {
            console.log('Connection closed');
        });
    });
}

async function main() {
    try {
        await checkRealtimeAccess();
        console.log('Verification of access to the Realtime API successful.');
    } catch (error) {
        if (error.message.includes('Invalid API key')) {
            console.log('Invalid API key for the Realtime API.');
        } else {
            console.log('An error occurred while verifying access to the Realtime API:', error.message);
        }
    }
}

main();
