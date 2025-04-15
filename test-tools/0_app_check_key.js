// 0_app_check_key.js
require('dotenv').config();
const { OpenAI } = require('openai');

async function main() {
    try {
        const client = new OpenAI({
            apiKey: process.env.OPENAI_API_KEY,
        });
        const models = await client.models.list();
        console.log('Valid API key. Available models:');
        for (const model of models.data) {
            console.log(model.id);
        }
    } catch (error) {
        if (error.message.includes('Invalid API key')) {
            console.log('Invalid API key.');
        } else {
            console.log('An error occurred:', error.message);
        }
    }
}

main();
