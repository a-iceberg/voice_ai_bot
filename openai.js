const { parsePhoneNumberFromString } = require('libphonenumber-js');
const WebSocket = require('ws');
const { v4: uuid } = require('uuid');
const { config, logger, logClient, logOpenAI } = require('./config');
const { sipMap, cleanupPromises } = require('./state');
const { streamAudio, rtpEvents } = require('./rtp');

logger.info('Loading openai.js module');

function sendFunctionResult(ws, call_id, outputText) {
  // output должен быть СТРОКОЙ
  const out = (typeof outputText === 'string') ? outputText : JSON.stringify(outputText);

  // 1) кладём вывод функции в беседу
  ws.send(JSON.stringify({
    type: 'conversation.item.create',
    item: {
      type: 'function_call_output',
      call_id,
      output: out
    }
  }));

  // 2) просим ассистента продолжить (обязательно!)
  ws.send(JSON.stringify({
    type: 'response.create',
    response: { modalities: ['audio','text'] }
  }));
}

// Асинхронная обработка вызова функции validateRussianPhone
function validateRussianPhone(raw) {
  // убираем пробелы, дефисы и скобки, если вдруг появились
  const cleaned = String(raw).replace(/[^\d+]/g, '');
  try {
    const pn = parsePhoneNumberFromString(cleaned, 'RU');
    // валидный ли номер и точно ли он российский
    if (pn?.isValid() && pn.country === 'RU') {
      return pn.number;            // вернёт строку формата +7XXXXXXXXXX
    }
  } catch (_) { /* ignore */ }
  return null;                      // невалидный
}

async function runValidatePhone(args) {
  const a = (typeof args === 'string') ? JSON.parse(args) : args;
  const raw = String(a?.phone ?? '');
  logger.info(`🔍 [PHONE] Валидация телефона через tools: "${raw}"`);   // +++

  const normalized = validateRussianPhone(raw);
  if (!normalized) {
    logger.warn(`[PHONE] Некорректный телефон: ${raw}`);                // +++
    return JSON.stringify({ ok: false, reason: 'invalid' });
  }
  logger.info(`[PHONE] Валидный телефон: ${normalized}`);               // +++
  return JSON.stringify({ ok: true, normalized });
}
async function handleValidatePhone(call, ws, logger) {
  if (!call.arguments) {
    logger.error('validate_phone: arguments missing');
    return;
  }

  let args;
  try {
    args = typeof call.arguments === 'string'
      ? JSON.parse(call.arguments)
      : call.arguments;
  } catch (e) {
    logger.error('validate_phone: bad JSON:', e);
    return;
  }

  const phone = String(args.phone);
  logger.info(`🔍 [PHONE] Валидация телефона через tools: "${phone}"`);

  const formattedPhone = validateRussianPhone(phone);

  if (!formattedPhone) {
    logger.warn(`[PHONE] Некорректный телефон: ${phone}`);
    if (ws && ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify({
        type: 'response.create',
        response: {
          modalities: ['audio', 'text'],
          instructions: `Скажи ровно: "Похоже, номер телефона некорректен. Пожалуйста, повторите номер полностью, начиная с +7."`,
          temperature: 0.6
        }
      }));
    }
  } else {
    logger.info(`[PHONE] Валидный телефон: ${formattedPhone}`);
    if (ws && ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify({
        type: 'response.create',
        response: {
          modalities: ['audio', 'text'],
          instructions: `Скажи ровно: "Я записала номер ${formattedPhone}. Всё верно?`,
          temperature: 0.6
        }
      }));
    }
  }
}

async function runValidateAddress(args) {
  const a = (typeof args === 'string') ? JSON.parse(args) : args;
  const city = String(a?.city || '').trim();
  const street = String(a?.street || '').trim();
  const house = String(a?.house_number || '').trim();

  const query = [city, street, house].filter(Boolean).join(', ');
  logger.info(`🔍 [ADDRESS] Валидация адреса: "${query}"`);
  if (!query) {
    logger.warn(`[ADDRESS] Пустой адрес, валидация не выполнена`);
    return JSON.stringify({ ok: false, reason: 'empty' });
  }

  try {
    const url = new URL('https://nominatim.openstreetmap.org/search');
    url.searchParams.set('q', query);
    url.searchParams.set('format', 'json');
    url.searchParams.set('limit', '1');
    const resp = await fetch(url, {
      headers: { 'User-Agent': 'IcebergBot/1.0 (asterisk_to_openai_rt)' },
      timeout: 10000
    });
    if (!resp.ok) {
      return JSON.stringify({ ok: false, reason: `http_${resp.status}` });
    }
    const data = await resp.json();
    if (!Array.isArray(data) || data.length === 0) {
      logger.warn(`[ADDRESS] Адрес не найден: "${query}"`);
      return JSON.stringify({ ok: false, reason: 'not_found' });
    }

    const hit = data[0];
    // извлечём нормализованные части, когда есть
    const display_name = hit.display_name;
    const latitude  = Number(hit.lat);
    const longitude = Number(hit.lon);

    // Пытаемся достать компоненты адреса (иногда приходят в hit.address)
    let norm = { city, street, house_number: house };
    if (hit.address) {
      norm.city         = hit.address.city || hit.address.town || hit.address.village || norm.city;
      norm.street       = hit.address.road || hit.address.pedestrian || norm.street;
      norm.house_number = hit.address.house_number || norm.house_number;
    }
    logger.info(`[ADDRESS] Валидный адрес: ${display_name} (${latitude}, ${longitude})`);
    return JSON.stringify({
      ok: true,
      normalized: { ...norm, latitude, longitude, display_name }
    });
  } catch (e) {
    logger.error(`[ADDRESS] Ошибка при валидации: ${e.message}`);
    return JSON.stringify({ ok: false, reason: 'exception' });
  }
}
// Асинхронная обработка вызова функции save_client_info
// --- save-client-info runner -----------------------------------------------
const { spawn } = require('child_process');

/**
 * Запускает save_client_info.py и логирует его вывод.
 * При наличии строки «✅ Номер новой заявки: <num>» вернёт orderNumber.
 */
async function runSaveClientInfo(clientData, logger) {
  return new Promise((resolve, reject) => {
    const proc = spawn('python3', ['-u', 'save_client_info.py'], { stdio: ['pipe', 'pipe', 'pipe'] });
    let orderNumber = null;

    proc.stdout.on('data', buf => {
      buf.toString().split(/\r?\n/).filter(Boolean).forEach(line => {
        logger.info(`[save_client_info] ${line}`);
        const m = line.match(/Номер новой заявки:\s*([^\s]+)/);
        if (m) orderNumber = m[1];
      });
    });

    proc.stderr.on('data', buf =>
      buf.toString().split(/\r?\n/).filter(Boolean)
        .forEach(line => logger.error(`[save_client_info:stderr] ${line}`))
    );

    proc.on('close', code => {
      if (code === 0 && orderNumber) return resolve(orderNumber);
      const msg = `save_client_info.py exited with code ${code}`;
      logger.error(msg);
      reject(new Error(msg));
    });

    proc.stdin.write(JSON.stringify(clientData));
    proc.stdin.end();
  });
}


// --- основной обработчик ----------------------------------------------------
async function handleSaveClientInfo(call, ws, logger) {
  /* ---------- 1. parse arguments ----------------------------------------- */
  if (!call.arguments) return logger.error('save_client_info: arguments missing');

  let args;
  try {
    args = typeof call.arguments === 'string'
      ? JSON.parse(call.arguments)
      : call.arguments;
  } catch (e) {
    return logger.error('save_client_info: bad JSON:', e);
  }

  /* ---------- 2. build payload for Python -------------------------------- */
  const channelEntry = Array.from(sipMap.entries()).find(([, data]) => data.ws === ws);
  const callerNumber = channelEntry ? channelEntry[1].callerNumber : null;

  const clientData = {
    name:  args.name,
    direction: args.direction,
    circumstances: args.circumstances || '',
    brand: args.brand || '',
    phone: String(args.phone),
    phone2: callerNumber || '',
    address: {
      city: args.address?.city,
      street: args.address?.street,
      house_number: args.address?.house_number,
      apartment: args.address?.apartment || '',
      entrance: args.address?.entrance || '',
      floor: args.address?.floor || '',
      intercom: args.address?.intercom || '',
      latitude: args.address?.latitude,
      longitude: args.address?.longitude
    },
    date: args.date || '',
    comment: args.comment || ''
  };

  /* ---------- 3. run Python ---------------------------------------------- */
  let orderNum;
  try {
    orderNum = await runSaveClientInfo(clientData, logger); // ← ловит «Пл2251279»
    logger.info(`Заявка создана, номер ${orderNum}`);
  } catch (err) {
    logger.error(`save_client_info: ${err.message}`);

    // вежливо сообщаем об ошибке
    if (ws?.readyState === ws.OPEN) {
      ws.send(JSON.stringify({
        type: 'response.create',
        response: {
          modalities: ['audio', 'text'],
          instructions: 'К сожалению, заявку сохранить не удалось. Попробуйте позже.'
        }
      }));
    }
    return;
  }

/* ---------- 4. tell the user the ticket number ------------------------- */
  if (ws && ws.readyState === ws.OPEN) {
    const reply = `Ваша заявка сохранена. Номер ${orderNum}. Спасибо за обращение!`;

    ws.send(
      JSON.stringify({
        type: 'response.create',
        response: {
          modalities: ['audio', 'text'],
          instructions: `Скажи ровно и обязательно озвучь номер заявки: "${reply}"`, // 🔒 фиксируем формулировку
          temperature: 0.6
        }
      })
    );

    logger.info(`🔔 [Client] Ответ с номером отправлен в OpenAI: ${orderNum}`);
  }}

async function waitForBufferEmpty(channelId, maxWaitTime = 6000, checkInterval = 10) {
  const channelData = sipMap.get(channelId);
  if (!channelData?.streamHandler) {
    logOpenAI(`No streamHandler for ${channelId}, proceeding`, 'info');
    return true;
  }
  const streamHandler = channelData.streamHandler;
  const startWaitTime = Date.now();

  let audioDurationMs = 1000; // Default minimum
  if (channelData.totalDeltaBytes) {
    audioDurationMs = Math.ceil((channelData.totalDeltaBytes / 8000) * 1000) + 500; // Audio duration + 500ms margin
  }
  const dynamicTimeout = Math.min(audioDurationMs, maxWaitTime);
  logOpenAI(`Using dynamic timeout of ${dynamicTimeout}ms for ${channelId} (estimated audio duration: ${(channelData.totalDeltaBytes || 0) / 8000}s)`, 'info');

  let audioFinishedReceived = false;
  const audioFinishedPromise = new Promise((resolve) => {
    rtpEvents.once('audioFinished', (id) => {
      if (id === channelId) {
        logOpenAI(`Audio finished sending for ${channelId} after ${Date.now() - startWaitTime}ms`, 'info');
        audioFinishedReceived = true;
        resolve();
      }
    });
  });

  const isBufferEmpty = () => (
    (!streamHandler.audioBuffer || streamHandler.audioBuffer.length === 0) &&
    (!streamHandler.packetQueue || streamHandler.packetQueue.length === 0)
  );
  if (!isBufferEmpty()) {
    let lastLogTime = 0;
    while (!isBufferEmpty() && (Date.now() - startWaitTime) < maxWaitTime) {
      const now = Date.now();
      if (now - lastLogTime >= 50) {
        logOpenAI(`Waiting for RTP buffer to empty for ${channelId} | Buffer: ${streamHandler.audioBuffer?.length || 0} bytes, Queue: ${streamHandler.packetQueue?.length || 0} packets`, 'info');
        lastLogTime = now;
      }
      await new Promise(resolve => setTimeout(resolve, checkInterval));
    }
    if (!isBufferEmpty()) {
      logger.warn(`Timeout waiting for RTP buffer to empty for ${channelId} after ${maxWaitTime}ms`);
      return false;
    }
    logOpenAI(`RTP buffer emptied for ${channelId} after ${Date.now() - startWaitTime}ms`, 'info');
  }

  const timeoutPromise = new Promise((resolve) => {
    setTimeout(() => {
      if (!audioFinishedReceived) {
        logger.warn(`Timeout waiting for audioFinished for ${channelId} after ${dynamicTimeout}ms`);
      }
      resolve();
    }, dynamicTimeout);
  });
  await Promise.race([audioFinishedPromise, timeoutPromise]);

  logOpenAI(`waitForBufferEmpty completed for ${channelId} in ${Date.now() - startWaitTime}ms`, 'info');
  return true;
}

async function startOpenAIWebSocket(channelId) {
  const OPENAI_API_KEY = config.OPENAI_API_KEY;
  if (!OPENAI_API_KEY) {
    logger.error('OPENAI_API_KEY is missing in config');
    throw new Error('Missing OPENAI_API_KEY');
  }

  let channelData = sipMap.get(channelId);
  if (!channelData) {
    throw new Error(`Channel ${channelId} not found in sipMap`);
  }

  let ws;
  let streamHandler = null;
  let retryCount = 0;
  const maxRetries = 3;
  let isResponseActive = false;
  let totalDeltaBytes = 0;
  let loggedDeltaBytes = 0;
  let segmentCount = 0;
  let responseBuffer = Buffer.alloc(0);
  let messageQueue = [];
  let itemRoles = new Map();
  let lastUserItemId = null;

  const processMessage = async (response) => {
    try {
      switch (response.type) {
        case 'session.created':
          logClient(`Session created for ${channelId}`);
          break;
        case 'session.updated':
          logOpenAI(`Session updated for ${channelId}`);
          break;
        case 'conversation.item.created':
          logOpenAI(`Conversation item created for ${channelId}`);
          if (response.item && response.item.id && response.item.role) {
            logger.debug(`Item created: id=${response.item.id}, role=${response.item.role} for ${channelId}`);
            itemRoles.set(response.item.id, response.item.role);
            if (response.item.role === 'user') {
              lastUserItemId = response.item.id;
              logOpenAI(`User voice command detected for ${channelId}, stopping current playback`);
              logger.debug(`VAD triggered - Full message for user voice command: ${JSON.stringify(response, null, 2)}`);
              if (streamHandler) {
                streamHandler.stopPlayback();
              }
            }
          }
          break;
        case 'response.created':
          logOpenAI(`Response created for ${channelId}`);
          break;
        
        case 'response.done': {
        const outputs = response?.response?.output || [];
        for (const out of outputs) {
          if (out.type === 'function_call') {
            const { name, call_id, arguments: args } = out;

            if (name === 'validate_phone') {
              logger.info(`[PHONE] function_call: ${args}`);
              const result = await runValidatePhone(args);
              sendFunctionResult(ws, call_id, result);
            }
            if (name === 'validate_address') {
              logger.info(`[ADDRESS] function_call: ${args}`);
              const result = await runValidateAddress(args);
              sendFunctionResult(ws, call_id, result);
            }

            if (name === 'save_client_info') {
              // args → это JSON с полями заявки (name, direction, phone, address, и т.д.)
              // запускаем ваш Python-скрипт и возвращаем номер заявки
              const a = (typeof args === 'string') ? JSON.parse(args) : args;

              // соберём clientData почти как в handleSaveClientInfo
              const channelEntry = Array.from(sipMap.entries()).find(([, data]) => data.ws === ws);
              const callerNumber = channelEntry ? channelEntry[1].callerNumber : null;
              const clientData = {
                name: a.name,
                direction: a.direction,
                circumstances: a.circumstances || '',
                brand: a.brand || '',
                phone: String(a.phone),
                phone2: callerNumber || '',
                address: {
                  city: a.address?.city,
                  street: a.address?.street,
                  house_number: a.address?.house_number,
                  apartment: a.address?.apartment || '',
                  entrance: a.address?.entrance || '',
                  floor: a.address?.floor || '',
                  intercom: a.address?.intercom || '',
                  latitude: a.address?.latitude,
                  longitude: a.address?.longitude
                },
                date: a.date || '',
                comment: a.comment || ''
              };

              try {
                const orderNum = await runSaveClientInfo(clientData, logger);
                // вернём как JSON-строку, чтобы модель могла красиво озвучить
                sendFunctionResult(ws, call_id, JSON.stringify({ ok: true, order_number: orderNum }));
              } catch (e) {
                sendFunctionResult(ws, call_id, JSON.stringify({ ok: false, error: 'save_failed' }));
              }
            }
          }
        }
        break;
}
        case 'response.audio.delta':
          if (response.delta) {
            const deltaBuffer = Buffer.from(response.delta, 'base64');
            if (deltaBuffer.length > 0 && !deltaBuffer.every(byte => byte === 0x7F)) {
              totalDeltaBytes += deltaBuffer.length;
              channelData.totalDeltaBytes = totalDeltaBytes; // Store in channelData
              sipMap.set(channelId, channelData);
              segmentCount++;
              if (totalDeltaBytes - loggedDeltaBytes >= 40000 || segmentCount >= 100) {
                logOpenAI(`Received audio delta for ${channelId}: ${deltaBuffer.length} bytes, total: ${totalDeltaBytes} bytes, estimated duration: ${(totalDeltaBytes / 8000).toFixed(2)}s`, 'info');
                loggedDeltaBytes = totalDeltaBytes;
                segmentCount = 0;
              }

              let packetBuffer = deltaBuffer;
              if (totalDeltaBytes === deltaBuffer.length) {
                const silenceDurationMs = config.SILENCE_PADDING_MS || 100;
                const silencePackets = Math.ceil(silenceDurationMs / 20);
                const silenceBuffer = Buffer.alloc(silencePackets * 160, 0x7F);
                packetBuffer = Buffer.concat([silenceBuffer, deltaBuffer]);
                logger.info(`Prepended ${silencePackets} silence packets (${silenceDurationMs} ms) for ${channelId}`);
              }

              if (sipMap.has(channelId) && streamHandler) {
                streamHandler.sendRtpPacket(packetBuffer);
              }
            } else {
              logger.warn(`Received empty or silent delta for ${channelId}`);
            }
          }
          break;
        case 'response.audio_transcript.delta':
          if (response.delta) {
            logger.debug(`Transcript delta for ${channelId}: ${response.delta.trim()}`);
            logger.debug(`Full transcript delta message: ${JSON.stringify(response, null, 2)}`);
          }
          break;
        case 'response.audio_transcript.done':
          if (response.transcript) {
            const role = response.item_id && itemRoles.get(response.item_id) ? itemRoles.get(response.item_id) : (lastUserItemId ? 'User' : 'Assistant');
            logger.debug(`Transcript done - Full message: ${JSON.stringify(response, null, 2)}`);
            if (role === 'User') {
              logOpenAI(`User command transcription for ${channelId}: ${response.transcript}`, 'info');
            } else {
              logOpenAI(`Assistant transcription for ${channelId}: ${response.transcript}`, 'info');
            }
          }
          break;
        case 'conversation.item.input_audio_transcription.delta':
          if (response.delta) {
            logger.debug(`User transcript delta for ${channelId}: ${response.delta.trim()}`);
            logger.debug(`Full user transcript delta message: ${JSON.stringify(response, null, 2)}`);
          }
          break;
        case 'conversation.item.input_audio_transcription.completed':
          if (response.transcript) {
            logger.debug(`User transcript completed - Full message: ${JSON.stringify(response, null, 2)}`);
            logOpenAI(`User command transcription for ${channelId}: ${response.transcript}`, 'info');
          }
          break;
        case 'response.audio.done':
          logOpenAI(`Response audio done for ${channelId}, total delta bytes: ${totalDeltaBytes}, estimated duration: ${(totalDeltaBytes / 8000).toFixed(2)}s`, 'info');
          isResponseActive = false;
          loggedDeltaBytes = 0;
          segmentCount = 0;
          itemRoles.clear();
          lastUserItemId = null;
          responseBuffer = Buffer.alloc(0);
          break;
        case 'error':
          logger.error(`OpenAI error for ${channelId}: ${response.error.message}`);
          ws.close();
          break;
        default:
          logger.debug(`Unhandled event type: ${response.type} for ${channelId}`);
          break;
      }
    } catch (e) {
      logger.error(`Error processing message for ${channelId}: ${e.message}`);
    }
  };

  const connectWebSocket = () => {
    return new Promise((resolve, reject) => {
      ws = new WebSocket(config.REALTIME_URL, {
        headers: {
          'Authorization': `Bearer ${OPENAI_API_KEY}`,
          'OpenAI-Beta': 'realtime=v1'
        }
      });

const tools = [
  {
    type: 'function',
    name: 'save_client_info',
    description: 'Создаёт заявку клиента в 1С и логирует данные',
    parameters: {
      type: 'object',
      required: ['name', 'direction', 'phone', 'address'],
      properties: {
        name:        { type: 'string',  description: 'Имя клиента' },
        direction:   { type: 'string',  description: 'цель / причина обращения',
          enum: [
            'Холодильники',
            'Кондиционеры',
            'Телевизоры',
            'Стиральные машины',
            'Посудомоечные машины',
            'Швейные машины',
            'Кофемашины',
            'Плиты',
            'Микроволновки',
            'Вытяжки',
            'Компьютеры',
            'Гаджеты',
            'Промышленный холод',
            'Газовые колонки',
            'Установка',
            'Пылесосы',
            'Клининг',
            'Дезинсекция',
            'Натяжные потолки',
            'Мелкобытовой сервис',
            'Ремонт квартир',
            'Сантехника',
            'Вывоз мусора',
            'Уборка',
            'Электрика',
            'Окна'
          ]
        },
        circumstances:{ type: 'string', description: 'Подробности неисправности / обращения' },
        brand:       { type: 'string',  description: 'Бренд и модель техники одной строкой' },
        phone: {
  type: 'string',
  description: 'Контактный телефон в формате +7XXXXXXXXXX',
  pattern: '^\\+7\\d{10}$'
},
        address: {
          type: 'object',
          description: 'Адрес выезда мастера',
          required: ['city', 'street', 'house_number'],
          properties: {
            city:        { type: 'string', description: 'Город' },
            street:      { type: 'string', description: 'Улица' },
            house_number:{ type: 'string', description: 'Дом / корпус / строение' },
            apartment:   { type: 'string', description: 'Квартира' },
            entrance:    { type: 'string', description: 'Подъезд' },
            floor:       { type: 'string', description: 'Этаж' },
            intercom:    { type: 'string', description: 'Код домофона' },
            latitude:    { type: 'number', description: 'Широта' },
            longitude:   { type: 'number', description: 'Долгота' }
          }
        },
        date:   { type: 'string', description: 'Желаемая дата визита (YYYY-MM-DD)' },
        comment:{ type: 'string', description: 'Дополнительный комментарий' }
      }
    }
  },
    {
    type: 'function',
    name: 'validate_phone',
    description: 'Валидирует и нормализует российский номер телефона.',
    parameters: {
      type: 'object',
      required: ['phone'],
      properties: {
        phone: {
          type: 'string',
          description: 'Контактный телефон, который произнёс клиент.',
        }
      }
    }
  },
  {
  type: 'function',
  name: 'validate_address',
  description: 'Проверяет адрес через Nominatim (OSM), возвращает нормализованный адрес и координаты.',
  parameters: {
    type: 'object',
    required: ['city','street','house_number'],
    properties: {
      city:        { type: 'string', description: 'Город' },
      street:      { type: 'string', description: 'Улица' },
      house_number:{ type: 'string', description: 'Дом / корпус / строение' }
    }
  }
}
];

      ws.on('open', async () => {
        logClient(`OpenAI WebSocket connected for ${channelId}`);
        ws.send(JSON.stringify({
          type: 'session.update',
          session: {
            modalities: ['audio', 'text'],
            voice: config.OPENAI_VOICE || 'alloy',
            instructions: config.SYSTEM_PROMPT,
            input_audio_format: 'g711_ulaw',
            output_audio_format: 'g711_ulaw',
            input_audio_transcription: {
              model: 'whisper-1',
              language: 'ru'
            },
            turn_detection: {
              type: 'server_vad',
              threshold: config.VAD_THRESHOLD || 0.6,
              prefix_padding_ms: config.VAD_PREFIX_PADDING_MS || 200,
              silence_duration_ms: config.VAD_SILENCE_DURATION_MS || 600
            },
            "temperature": 0.6,
            tools,
            tool_choice: 'auto'
          }
        }));
        logClient(`Session updated for ${channelId}`);

        try {
          const rtpSource = channelData.rtpSource || { address: '127.0.0.1', port: 12000 };
          streamHandler = await streamAudio(channelId, rtpSource);
          channelData.ws = ws;
          channelData.streamHandler = streamHandler;
          channelData.totalDeltaBytes = 0; // Initialize totalDeltaBytes
          sipMap.set(channelId, channelData);

          const itemId = uuid().replace(/-/g, '').substring(0, 32);
          logClient(`Sending initial message for ${channelId}: ${config.INITIAL_MESSAGE || 'Hi'}`);
          ws.send(JSON.stringify({
            type: 'conversation.item.create',
            item: {
              id: itemId,
              type: 'message',
              role: 'user',
              content: [{ type: 'input_text', text: config.INITIAL_MESSAGE || 'Hi' }]
            }
          }));
          ws.send(JSON.stringify({
            type: 'response.create',
            response: {
              modalities: ['audio', 'text'],
              instructions: config.SYSTEM_PROMPT,
              output_audio_format: 'g711_ulaw'
            }
          }));
          logClient(`Requested response for ${channelId}`);
          isResponseActive = true;
          resolve(ws);
        } catch (e) {
          logger.error(`Error setting up WebSocket for ${channelId}: ${e.message}`);
          reject(e);
        }
      });

      ws.on('message', (data) => {
        try {
          const response = JSON.parse(data.toString());
          logger.debug(`Raw WebSocket message for ${channelId}: ${JSON.stringify(response, null, 2)}`);
          messageQueue.push(response);
        } catch (e) {
          logger.error(`Error parsing WebSocket message for ${channelId}: ${e.message}`);
        }
      });

      ws.on('error', (e) => {
        logger.error(`WebSocket error for ${channelId}: ${e.message}`);
        if (retryCount < maxRetries && sipMap.has(channelId)) {
          retryCount++;
          setTimeout(() => connectWebSocket().then(resolve).catch(reject), 1000);
        } else {
          reject(new Error(`Failed WebSocket after ${maxRetries} attempts`));
        }
      });

      const handleClose = () => {
        logger.info(`WebSocket closed for ${channelId}`);
        channelData.wsClosed = true;
        channelData.ws = null;
        sipMap.set(channelId, channelData);
        ws.off('close', handleClose);
        const cleanupResolve = cleanupPromises.get(`ws_${channelId}`);
        if (cleanupResolve) {
          cleanupResolve();
          cleanupPromises.delete(`ws_${channelId}`);
        }
      };
      ws.on('close', handleClose);
    });
  };

  setInterval(async () => {
    const maxMessages = 5;
    for (let i = 0; i < maxMessages && messageQueue.length > 0; i++) {
      await processMessage(messageQueue.shift());
    }
  }, 25);

  try {
    await connectWebSocket();
  } catch (e) {
    logger.error(`Failed to start WebSocket for ${channelId}: ${e.message}`);
    throw e;
  }
}

module.exports = { startOpenAIWebSocket };
