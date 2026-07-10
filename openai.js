const { parsePhoneNumberFromString } = require('libphonenumber-js');
const WebSocket = require('ws');
const { v4: uuid } = require('uuid');
const { config, logger, logClient, logOpenAI } = require('./config');
const { sipMap, cleanupPromises, extMap } = require('./state');
const { streamAudio, rtpEvents } = require('./rtp');
const { handoffToOperator } = require('./asterisk');
const { findNearestAffilate, classifyZone, cityFromAffilate } = require('./address_zoning');
const { ProxyAgent, fetch: undiciFetch } = require('undici');

logger.info('Loading openai.js module');

const DD_PROXY_DISPATCHER = config.DD_HTTP_PROXY
  ? new ProxyAgent(config.DD_HTTP_PROXY)
  : undefined;
if (DD_PROXY_DISPATCHER) {
  logger.info(`[ADDRESS] DaData fetch via proxy: ${config.DD_HTTP_PROXY}`);
} else {
  logger.info('[ADDRESS] DaData fetch: direct (no proxy configured)');
}

//-----------------------------
const { DateTime } = require("luxon");
const TZ = "Europe/Moscow";

let DID_CITY = { default: 'msk', map: {} };
try {
  DID_CITY = require('./data/did_city.json');
  if (!DID_CITY.map) DID_CITY.map = {};
  if (!DID_CITY.default) DID_CITY.default = 'msk';
  logger.info(`[DID_CITY] loaded ${Object.keys(DID_CITY.map).length} DID mappings, default="${DID_CITY.default}"`);
} catch (e) {
  logger.warn(`[DID_CITY] не удалось загрузить did_city: ${e.message} — используется дефолт "msk"`);
}

function normalizeDid(raw) {
  const digits = String(raw || '').replace(/\D/g, '');
  if (digits.length === 11 && (digits[0] === '7' || digits[0] === '8')) return digits.slice(1);
  return digits;
}

function cityByDid(raw) {
  const key = normalizeDid(raw);
  return (key && DID_CITY.map[key]) || DID_CITY.default || 'msk';
}

// ----------------------------
// µ-law decode table + silence detector
const MULAW_DECODE_TABLE = (() => {
  const t = new Int16Array(256);
  for (let b = 0; b < 256; b++) {
    const inv = ~b & 0xFF;
    const sign = (inv & 0x80) ? -1 : 1;
    const exponent = (inv >> 4) & 0x07;
    const mantissa = inv & 0x0F;
    const sample = ((mantissa << 3) + 0x84) << exponent;
    t[b] = sign * (sample - 0x84);
  }
  return t;
})();

function isMuLawSilence(buf, rmsThreshold = 100, nearZeroFraction = 0.97) {
  if (!buf || buf.length === 0) return true;
  let sumSq = 0;
  let nearZero = 0;
  for (let i = 0; i < buf.length; i++) {
    const sample = MULAW_DECODE_TABLE[buf[i]];
    sumSq += sample * sample;
    if (sample > -50 && sample < 50) nearZero++;
  }
  const rms = Math.sqrt(sumSq / buf.length);
  return rms < rmsThreshold || nearZero / buf.length >= nearZeroFraction;
}

function isOperatorOffHours(dt = DateTime.now().setZone(TZ)) {
  const h = dt.hour;
  return h >= 23 || h < 3;
}

const OPERATOR_OFFHOURS_INSTRUCTION =
  'Скажи клиенту ровно по смыслу: «Для уточнения дальнейшей информации вы можете перезвонить операторам колл-центра с 7 до 23 часов. Они ответят на все ваши вопросы» и вежливо завершите разговор. НЕ обещай перевод на оператора прямо сейчас.';

function buildSystemPromptFinal() {
  const now = DateTime.now().setZone(TZ);
  const nowDateISO = now.toISODate();
  const nowWeekday = now.setLocale("ru").toFormat("cccc");
  const nowTime = now.toFormat("HH:mm");
  const baseSystemPrompt = process.env.SYSTEM_PROMPT || '';
  const lines = [
    `Сегодня: ${nowDateISO} (${nowWeekday}). Текущее время по Москве: ${nowTime}. Текущий часовой пояс: ${TZ}.`,
    `Если клиент говорит "сегодня", "завтра", "послезавтра" или относительные выражения — вычисли конкретную дату в формате YYYY-MM-DD относительно ${nowDateISO}.`,
    `ВАЖНО: при озвучивании даты говори её словами по-русски (пример: 2025-12-16 → "16 декабря 2025 года"), но в вызовах инструментов (например save_client_info) используй формат YYYY-MM-DD.`,
  ];
  if (isOperatorOffHours(now)) {
    lines.push(
      'ВАЖНО: сейчас ночное время — операторы колл-центра НЕ работают. НЕ обещай и НЕ выполняй перевод на оператора. Если по правилам потребовался бы перевод на оператора, вместо этого скажи: «Для уточнения дальнейшей информации вы можете перезвонить операторам колл-центра с 7 до 23 часов. Они ответят на все ваши вопросы».'
    );
  }
  lines.push(baseSystemPrompt);
  return lines.join("\n\n");
}

function formatRuDateSpeech(iso) {
  if (!iso || !/^\d{4}-\d{2}-\d{2}$/.test(String(iso))) return iso;
  return DateTime.fromISO(String(iso), { zone: TZ })
    .setLocale("ru")
    .toFormat("d LLLL");
}

function clientLocalDateInfo(tzStr) {
  let zone = TZ;
  const t = String(tzStr || '').trim();
  if (/^UTC[+-]\d{1,2}(:\d{2})?$/.test(t)) zone = t;
  let now = DateTime.now().setZone(zone);
  if (!now.isValid) {
    zone = TZ;
    now = DateTime.now().setZone(TZ);
  }
  return {
    zone,
    nowTime: now.toFormat('HH:mm'),
    todayISO: now.toISODate() || '',
    tomorrowISO: now.plus({ days: 1 }).toISODate() || '',
    withinWorkingHours: now.hour >= 0 && now.hour < 19,
  };
}
//-----------------------------
// --- Correction detector ---
const CORRECTION_PATTERNS = [
  /\bне\s?верно\b/i,
  /\bнеправил/i,
  /\bнет[, ]/i,
  /\bдруг(ой|ой\s+адрес|ой\s+номер)\b/i,
  /\bисправ(ь|ьте)\b/i,
  /\bне то\b/i,
  /\bзаписал[аи]? не так\b/i,
  /\bповтор(и|ите)\b/i
];



function hasCorrectionIntent(text) {
  const t = (text || '').toLowerCase();
  return CORRECTION_PATTERNS.some(rx => rx.test(t));
}

const DIRECTION_KEYWORDS = [
  [/посудомо|посудомойк/i, 'Посудомоечные машины'],
  [/стиральн|стиралк/i, 'Стиральные машины'],
  [/швейн/i, 'Швейные машины'],
  [/кофемашин|кофеварк|кофе[-\s]?машин/i, 'Кофемашины'],
  [/микроволнов|\bсвч\b|микроволновк/i, 'Микроволновки'],
  [/холодильник|морозильник|холодос/i, 'Холодильники'],
  [/промышленн\w*\s+холод|промхолод/i, 'Промышленный холод'],
  [/кондиционер|сплит[-\s]?систем|кондей/i, 'Кондиционеры'],
  [/телевизор|телек|\bтв\b/i, 'Телевизоры'],
  [/вытяжк/i, 'Вытяжки'],
  [/газов\w*\s+колонк|\bколонк/i, 'Газовые колонки'],
  [/плит[аоуы]|варочн|духовк|духов\b/i, 'Плиты'],
  [/компьютер|ноутбук|\bпк\b|\bкомп\b/i, 'Компьютеры'],
  [/смартфон|планшет|гаджет/i, 'Гаджеты'],
  [/натяжн\w*\s+потол|потолок|потолк/i, 'Натяжные потолки'],
  [/дезинсекц|таракан|\bклоп|насеком/i, 'Дезинсекция'],
  [/клининг/i, 'Клининг'],
  [/сантехник|смеситель|\bкран\b|унитаз|засор|\bтруб/i, 'Сантехника'],
  [/электрик|проводк|розетк|электричеств/i, 'Электрика'],
  [/вскрыт\w*\s+замк|\bзамок\b|\bзамк[аи]/i, 'Вскрытие замков'],
  [/\bокно\b|\bокн[аоуе]\b|стеклопакет/i, 'Окна'],
  [/вывоз\w*\s+мусор|\bмусор/i, 'Вывоз мусора партнеры'],
  [/уборк/i, 'Уборка'],
  [/ремонт\w*\s+квартир|квартир/i, 'Ремонт квартир'],
  [/установк/i, 'Установка'],
  [/мелкобытов|утюг|блендер|миксер|\bфен\b|чайник|мультиварк/i, 'Мелкобытовой сервис'],
];

function detectDirection(text) {
  const t = String(text || '');
  for (const [rx, name] of DIRECTION_KEYWORDS) {
    if (rx.test(t)) return name;
  }
  return '';
}

function normalizePhoneToE164Like(input) {
  if (!input) return '';
  const digits = String(input).replace(/\D+/g, '');
  if (digits.length === 11 && digits.startsWith('8')) return '7' + digits.slice(1);
  if (digits.length === 10) return '7' + digits;
  return digits; // уже с кодом страны
}

function normalizeAddressArgs(obj = {}) {
  // мягкая нормализация: трим/нижний регистр для текстовых полей
  const pick = (v) => (typeof v === 'string' ? v.trim().toLowerCase() : v || '');
  return {
    city: pick(obj.city),
    street: pick(obj.street),
    house_number: pick(obj.house_number),
    apartment: pick(obj.apartment),
    // при желании: корпус/строение и т.п.
  };
}

// поверхностное равенство по строке JSON
const shallowEqualJSON = (a, b) => JSON.stringify(a) === JSON.stringify(b);

const REPAIRABLE_TOOLS = {
  validate_address: {
    slot: 'address',
    takeCurrent: (args) => {
      const obj = typeof args === 'string' ? JSON.parse(args) : (args || {});
      return { raw: obj, norm: normalizeAddressArgs(obj) };
    },
    takePreviousFromCh: (ch) => ch.lastToolArgs?.address ?? null,
    saveToCh: (ch, curr) => {
      ch.lastToolArgs = ch.lastToolArgs || {};
      ch.lastToolArgs.address = curr; // храним и raw, и norm
    },
    isSame: (prev, curr) => !!prev && !!curr && shallowEqualJSON(prev.norm, curr.norm),
    looksInvalid: (curr) => {
      const { city, street } = curr.norm || {};
      return !city || !street; // мягкая эвристика
    },
    hint: 'Пересобери адрес по последним репликам (город, улица, дом, кв) и вызови validate_address с новыми аргументами (не используй старые).',
  },
};

async function triggerSelfRepair(ws, channelId, toolName, currArgs, reason, enqueueResponseCreate) {
  const cfg = REPAIRABLE_TOOLS[toolName];
  const hint = cfg?.hint || 'Пересобери аргументы по последним репликам и повторно вызови инструмент.';

  // берём короткое окно последних реплик
  const ch = sipMap.get(channelId) || {};
  const lines = (ch.transcriptWindow || [])
    .map(x => `${x.role === 'user' ? 'Клиент' : 'Ассистент'}: ${x.text}`)
    .slice(-8).join('\n');

  ws.send(JSON.stringify({
    type: 'conversation.item.create',
    item: {
      type: 'message',
      role: 'user',
      content: [{
        type: 'input_text',
        text:
`[СЛУЖЕБНО] Авто-ремонт tool=${toolName} (reason=${reason}).
Последние реплики:
${lines}

Предыдущие аргументы (нормализованные/сырые):
${JSON.stringify(currArgs)}

Инструкция:
${hint}`
      }]
    }
  }));

  if (typeof enqueueResponseCreate === 'function') {
    enqueueResponseCreate({
      output_modalities: ['audio'],
      instructions: `Выполни переразбор последних реплик и повторно вызови ${toolName} уже с исправленными аргументами.`,
    });
  } else {
    ws.send(JSON.stringify({
      type: 'response.create',
      response: {
        output_modalities: ['audio'],
        instructions: `Выполни переразбор последних реплик и повторно вызови ${toolName} уже с исправленными аргументами.`,
      }
    }));
  }
} 

function sendFunctionResult(ws, call_id, outputText, enqueueResponseCreate, opts = {}) {
  const out = (typeof outputText === 'string') ? outputText : JSON.stringify(outputText);

  ws.send(JSON.stringify({
    type: 'conversation.item.create',
    item: {
      type: 'function_call_output',
      call_id,
      output: out
    }
  }));

  const createResponse = (opts.createResponse !== false);

  if (createResponse) {
    if (typeof enqueueResponseCreate === 'function') {
      enqueueResponseCreate({ output_modalities: ['audio'] });
    } else {
      ws.send(JSON.stringify({
        type: 'response.create',
        response: { output_modalities: ['audio'] }
      }));
    }
  }
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
  logger.info(`🔍 [PHONE] Валидация телефона через tools: "${raw}"`); 

  const normalized = validateRussianPhone(raw);
  if (!normalized) {
    logger.warn(`[PHONE] Некорректный телефон: ${raw}`);                
    return JSON.stringify({ ok: false, reason: 'invalid' });
  }
  logger.info(`[PHONE] Валидный телефон: ${normalized}`);               
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
          output_modalities: ['audio'],
          instructions: `Скажи ровно: "Похоже, номер телефона некорректен. Пожалуйста, повторите номер полностью, сейчас именно по одной цифре, начиная с +7."`
        }
      }));
    }
  } else {
    logger.info(`[PHONE] Валидный телефон: ${formattedPhone}`);
    if (ws && ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify({
        type: 'response.create',
        response: {
          output_modalities: ['audio'],
          instructions: `Скажи ровно, произнося точно по одной цифре (например +7 9 0 9 5 6 2 8 4 2 0): "Я записал номер ${formattedPhone}. Всё верно?`
        }
      }));
    }
  }
}
/** Сохраняет результат validate_address на канале для озвучки (spoken_city) vs 1С (system_city). */
function applyLastValidatedAddress(ch, normalized) {
  if (!normalized) return ch;
  ch = ch || {};
  ch.lastValidatedAddress = {
    spoken_city: normalized.spoken_city || normalized.city || '',
    system_city: normalized.city || '',
    street: normalized.street || '',
    house_number: normalized.house_number || '',
    display_name: normalized.display_name || '',
    timezone: normalized.timezone || '',
    affilate: normalized.affilate || '',
  };
  return ch;
}

/**
 * @param {string} query Адрес для стандартизации
 * @returns {Promise<string>}
 */
async function fetchDaDataTimezone(query) {
  const q = String(query || '').trim();
  if (!q) return '';
  if (!config.DD_API_KEY || !config.DD_SECRET_KEY) {
    logger.warn('[ADDRESS] Нет DD_API_KEY/DD_SECRET_KEY — таймзона через clean-API недоступна');
    return '';
  }
  try {
    const resp = await undiciFetch(
      'https://cleaner.dadata.ru/api/v1/clean/address',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'Authorization': `Token ${config.DD_API_KEY}`,
          'X-Secret': config.DD_SECRET_KEY,
        },
        body: JSON.stringify([q]),
        dispatcher: DD_PROXY_DISPATCHER,
      }
    );
    if (!resp.ok) {
      logger.warn(`[ADDRESS] DaData clean HTTP ${resp.status} — таймзона не получена`);
      return '';
    }
    const json = /** @type {any} */ (await resp.json());
    const row = Array.isArray(json) ? json[0] : null;
    return String(row?.timezone || '').trim();
  } catch (e) {
    logger.warn(`[ADDRESS] DaData clean exception: ${e.message} — таймзона не получена`);
    return '';
  }
}

async function runValidateAddress(args) {
  const a = (typeof args === 'string') ? JSON.parse(args) : args;
  const city = String(a?.city || '').trim();
  const street = String(a?.street || '').trim();
  const house = String(a?.house_number || '').trim();
  const direction = String(a?.direction || '').trim();

  const fullAddress = [city, street, house].filter(Boolean).join(', ');
  logger.info(`🔍 [ADDRESS] Валидация адреса (DaData): "${fullAddress}", direction="${direction}"`);

  if (!fullAddress) {
    logger.warn(`[ADDRESS] Пустой адрес, валидация не выполнена`);
    return JSON.stringify({ ok: false, reason: 'empty' });
  }

  if (!config.DD_API_KEY) {
    logger.error(`[ADDRESS] DD_API_KEY отсутствует — DaData недоступен`);
    return JSON.stringify({ ok: false, reason: 'no_dadata_key' });
  }

  let suggestions = [];
  try {
    const resp = await undiciFetch(
      'https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/address',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'Authorization': `Token ${config.DD_API_KEY}`,
        },
        body: JSON.stringify({ query: fullAddress, count: 5 }),
        dispatcher: DD_PROXY_DISPATCHER,
      }
    );
    if (!resp.ok) {
      logger.error(`[ADDRESS] DaData HTTP ${resp.status}`);
      return JSON.stringify({ ok: false, reason: `http_${resp.status}` });
    }
    const json = /** @type {any} */ (await resp.json());
    suggestions = Array.isArray(json?.suggestions) ? json.suggestions : [];
  } catch (e) {
    logger.error(`[ADDRESS] DaData exception: ${e.message}`);
    return JSON.stringify({ ok: false, reason: 'exception' });
  }

  let pick = null;
  const seenValues = new Set();
  for (const loc of suggestions) {
    const d = loc?.data || {};
    if (d.fias_level !== '8') continue;
    if (d.qc_geo !== '0' && d.qc_geo !== '1') continue;
    const val = loc?.unrestricted_value;
    if (!val || seenValues.has(val)) continue;
    seenValues.add(val);
    pick = loc;
    break;
  }

  if (!pick) {
    logger.warn(`[ADDRESS] DaData: подходящий suggest не найден для "${fullAddress}"`);
    return JSON.stringify({ ok: false, reason: 'geo_unresolved' });
  }

  const latitude = Number(pick.data.geo_lat);
  const longitude = Number(pick.data.geo_lon);
  const display_name = String(pick.unrestricted_value || fullAddress);

  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    logger.warn(`[ADDRESS] DaData: координаты невалидны для "${fullAddress}"`);
    return JSON.stringify({ ok: false, reason: 'geo_unresolved' });
  }

  const { distance, affilate } = findNearestAffilate(latitude, longitude);
  const { zone } = classifyZone({ affilate, distance, direction });
  const localityCity = cityFromAffilate(affilate);

  // Реальный город/населённый пункт из ответа DaData — для озвучки клиенту.
  // localityCity — это «филиальный» город (для маршрутизации заявки в 1С),
  // он может отличаться от реального (например, «Москва» вместо «Долгопрудный»).
  const dd = pick.data || {};
  const spokenCity = String(
    dd.city || dd.settlement || dd.area || dd.region || city || ''
  ).trim();
  const ddTimezone = await fetchDaDataTimezone(display_name);

  logger.info(
    `[ADDRESS] DaData OK: "${display_name}" (${latitude},${longitude}) | nearest=${affilate || 'none'} | dist=${Number.isFinite(distance) ? distance.toFixed(2) : 'inf'}km | direction="${direction}" | zone=${zone} | spoken_city="${spokenCity}"`
  );

  if (zone === 'out_of_service') {
    return JSON.stringify({
      ok: false,
      reason: 'out_of_service_zone',
      message:
        'Указанный клиентом адрес находится вне зоны работы нашей компании. Вежливо донесите это до клиента и прекратите далее оформлять заявку!',
      normalized: { display_name, latitude, longitude, spoken_city: spokenCity },
    });
  }

  const normalized = {
    city: localityCity,
    spoken_city: spokenCity,
    street,
    house_number: house,
    latitude,
    longitude,
    display_name,
    timezone: ddTimezone,
    affilate,
  };

  if (zone === 'paid') {
    return JSON.stringify({
      ok: true,
      transfer: true,
      reason: 'paid_zone',
      message:
        'Указанный клиентом адрес находится вне зоны бесплатного выезда мастера, звонок был переведен на оператора колл-центра для уточнения условий.',
      normalized,
    });
  }

  return JSON.stringify({ ok: true, normalized });
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

function formatRuPhoneMasked(normalized) {
  // ожидаем что normalized типа "+7ХХХХХХХХХХ" или "7ХХХХХХХХХХ"
  const digits = String(normalized).replace(/\D/g, "");

  // приводим к 11 цифрам, начинающимся на 7
  let d = digits;
  if (d.length === 11 && d.startsWith("8")) d = "7" + d.slice(1);
  if (!(d.length === 11 && d.startsWith("7"))) return null;

  const a = d.slice(1, 4);
  const b = d.slice(4, 7);
  const c = d.slice(7, 9);
  const e = d.slice(9, 11);

  return `+7 ${a} ${b} ${c} ${e}`; // "+7 ХХХ ХХХ ХХ ХХ"
}

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
        resolve(undefined);
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
      resolve(undefined);
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
  let initialGreetingItemId = null;
  let transcriptWindow = [];
function pushTranscript(role, text) {
  transcriptWindow.push({ role, text, t: Date.now() });
  if (transcriptWindow.length > 10) transcriptWindow.shift();
}

  const SILENCE_GREETING_TIMEOUT_MS = (config.SILENCE_GREETING_TIMEOUT_SEC || 0) * 1000;
  const SILENCE_CONVO_TIMEOUT_MS = (config.SILENCE_CONVO_TIMEOUT_SEC || 0) * 1000;
  const SILENCE_CONTINUE_WAIT_MS = (config.SILENCE_CONTINUE_WAIT_SEC || 120) * 1000;
  const SILENCE_INAUDIBLE_HANGUP_MS = (config.SILENCE_INAUDIBLE_HANGUP_SEC || 5) * 1000;
  const PHRASE_INAUDIBLE = 'Извините, Вас не слышно, повторите, пожалуйста.';
  const PHRASE_CONTINUE = 'Вы готовы продолжить диалог?';

  let inactivityTimer = null;
  let hasClientSpoken = false;
  let pendingAfterPrompt = null;
  let conversationSilenceDrop = false;

  function clearInactivityTimer() {
    if (inactivityTimer) {
      clearTimeout(inactivityTimer);
      inactivityTimer = null;
    }
  }

  function armInactivityTimer(ms, fn) {
    clearInactivityTimer();
    if (!ms || ms <= 0) return;
    inactivityTimer = setTimeout(() => {
      inactivityTimer = null;
      if (!sipMap.has(channelId)) return;
      try { fn(); } catch (e) { logger.error(`[SILENCE] timer cb error for ${channelId}: ${e.message}`); }
    }, ms);
  }

  function noteClientActivity() {
    hasClientSpoken = true;
    pendingAfterPrompt = null;
    awaitingPlaybackEnd = false;
    scheduledSilenceAction = null;
    conversationSilenceDrop = false;
    clearPlaybackFallback();
    clearInactivityTimer();
  }

  function speakInactivityPhrase(text) {
    try {
      if (ws && ws.readyState === ws.OPEN) {
        ws.send(JSON.stringify({
          type: 'conversation.item.create',
          item: {
            type: 'message',
            role: 'assistant',
            content: [{ type: 'output_text', text: `Произнеси клиенту дословно, не повторяя предыдущие фразы: "${text}"` }],
          },
        }));
      }
    } catch (e) {
      logger.error(`[SILENCE] failed to add inactivity item for ${channelId}: ${e.message}`);
    }
    enqueueResponseCreate({
      output_modalities: ['audio'],
      instructions: `Произнеси дословно: "${text}"`,
    });
  }

  function requestCallbackTask() {
    try {
      const ch = sipMap.get(channelId) || {};
      const callerNumber = ch.callerNumber || '';
      const callerNorm = callerNumber ? (validateRussianPhone(callerNumber) || callerNumber) : '';
      const mainPhone = String(ch.lastValidatedPhone || callerNorm || '');
      const addr = ch.lastValidatedAddress || {};
      const addrCity = addr.affilate ? (String(addr.affilate).split('_')[1] || '') : '';
      const cityCode = addrCity || cityByDid(ch.phoneBot);

      const clientMessage = (ch.transcriptWindow || [])
        .filter(x => x && x.role === 'user' && x.text)
        .map(x => String(x.text).trim())
        .filter(Boolean)
        .join('; ');

      const payload = {
        name: ch.lastName || '',
        phone: mainPhone,
        direction: ch.lastDirection || '',
        brand: ch.lastBrand || '',
        circumstances: ch.lastCircumstances || '',
        comment: ch.lastComment || '',
        message: clientMessage,
        city: cityCode,
        did: ch.phoneBot || '',
      };

      logger.info(`[SILENCE] Создаю заявку на обратный звонок для ${channelId}: phone=${payload.phone}, direction=${payload.direction}, city=${payload.city}, did=${payload.did}`);

      const proc = spawn('python3', ['-u', 'request_call.py'], { stdio: ['pipe', 'pipe', 'pipe'] });
      proc.stdout.on('data', d => String(d).split('\n').filter(Boolean).forEach(line => logger.info(`[request_call] ${line}`)));
      proc.stderr.on('data', d => String(d).split('\n').filter(Boolean).forEach(line => logger.error(`[request_call:stderr] ${line}`)));
      proc.on('error', e => logger.error(`[request_call] spawn error для ${channelId}: ${e.message}`));
      proc.stdin.write(JSON.stringify(payload));
      proc.stdin.end();
    } catch (e) {
      logger.error(`[SILENCE] Не удалось создать заявку на обратный звонок для ${channelId}: ${e.message}`);
    }
  }

  function endCallAfterPhrase(tag = 'END') {
    clearInactivityTimer();
    clearPlaybackFallback();
    let done = false;
    let fallback = null;
    const finish = async (id) => {
      if (done || id !== channelId) return;
      done = true;
      rtpEvents.off('audioFinished', finish);
      if (fallback) clearTimeout(fallback);
      logger.info(`[${tag}] Прощальная фраза проиграна — завершаю звонок ${channelId}`);
      try { if (ws && ws.readyState === ws.OPEN) ws.close(); } catch {}
      try {
        const { cleanupChannel } = require('./asterisk');
        await cleanupChannel(channelId);
      } catch (e) {
        logger.error(`[${tag}] cleanupChannel не удался для ${channelId}: ${e.message}`);
      }
    };
    fallback = setTimeout(() => finish(channelId), 20000);
    rtpEvents.on('audioFinished', finish);
  }

  async function dropCallForSilence() {
    logger.info(`[SILENCE] Клиент молчит — сбрасываю звонок ${channelId}`);
    clearInactivityTimer();
    if (conversationSilenceDrop) {
      conversationSilenceDrop = false;
      requestCallbackTask();
    }
    try { if (ws && ws.readyState === ws.OPEN) ws.close(); } catch {}
    try {
      const { cleanupChannel } = require('./asterisk');
      await cleanupChannel(channelId);
    } catch (e) {
      logger.error(`[SILENCE] cleanupChannel не удался для ${channelId}: ${e.message}`);
    }
  }

  function onSilenceTimeout() {
    if (!hasClientSpoken) {
      logger.info(`[SILENCE] Сценарий A: молчание после приветствия для ${channelId} -> "Вас не слышно"`);
      pendingAfterPrompt = 'arm_hangup';
      speakInactivityPhrase(PHRASE_INAUDIBLE);
    } else {
      logger.info(`[SILENCE] Сценарий B: молчание в диалоге для ${channelId} -> "Готовы продолжить?"`);
      pendingAfterPrompt = 'arm_continue';
      speakInactivityPhrase(PHRASE_CONTINUE);
    }
  }

  function onContinueTimeout() {
    logger.info(`[SILENCE] Нет ответа после "Готовы продолжить?" для ${channelId} -> "Вас не слышно"`);
    conversationSilenceDrop = true;
    pendingAfterPrompt = 'arm_hangup';
    speakInactivityPhrase(PHRASE_INAUDIBLE);
  }

  let awaitingPlaybackEnd = false;
  let scheduledSilenceAction = null;  // 'hangup' | 'continue' | 'silence'
  let playbackFallbackTimer = null;

  function clearPlaybackFallback() {
    if (playbackFallbackTimer) {
      clearTimeout(playbackFallbackTimer);
      playbackFallbackTimer = null;
    }
  }

  function applyScheduledArm() {
    if (!awaitingPlaybackEnd) return;
    awaitingPlaybackEnd = false;
    clearPlaybackFallback();
    if (!sipMap.has(channelId)) return;
    const action = scheduledSilenceAction;
    scheduledSilenceAction = null;
    if (action === 'hangup') {
      armInactivityTimer(SILENCE_INAUDIBLE_HANGUP_MS, dropCallForSilence);
    } else if (action === 'continue') {
      armInactivityTimer(SILENCE_CONTINUE_WAIT_MS, onContinueTimeout);
    } else {
      armInactivityTimer(hasClientSpoken ? SILENCE_CONVO_TIMEOUT_MS : SILENCE_GREETING_TIMEOUT_MS, onSilenceTimeout);
    }
  }

  function scheduleInactivityAfterSpeech() {
    if (pendingAfterPrompt === 'arm_hangup') {
      pendingAfterPrompt = null;
      scheduledSilenceAction = 'hangup';
    } else if (pendingAfterPrompt === 'arm_continue') {
      pendingAfterPrompt = null;
      scheduledSilenceAction = 'continue';
    } else {
      scheduledSilenceAction = 'silence';
    }
    awaitingPlaybackEnd = true;
    clearInactivityTimer();
    clearPlaybackFallback();
    const tailMs = Math.min(((channelData?.lastAudioBytes || 0) / 8000) * 1000 + 1500, 60000);
    playbackFallbackTimer = setTimeout(applyScheduledArm, Math.max(tailMs, 1500));
  }

  const onAudioFinishedForSilence = (id) => {
    if (id !== channelId) return;
    applyScheduledArm();
  };
  rtpEvents.on('audioFinished', onAudioFinishedForSilence);

  // ─────────────────────────────────────────────
  // response.create queue (anti "active response")
  const RESPONSE_MIN_INTERVAL_MS = 200;
  let lastResponseCreateAt = 0;

  // Очередь payload-ов (instructions/temperature/etc).
  const responseCreateQueue = [];

  const canSendNow = () => (
    ws && ws.readyState === ws.OPEN &&
    !isResponseActive &&
    (Date.now() - lastResponseCreateAt) >= RESPONSE_MIN_INTERVAL_MS
  );

  function _sendResponseCreateNow(payload = {}) {
    if (!ws || ws.readyState !== ws.OPEN) return;

    isResponseActive = true;
    lastResponseCreateAt = Date.now();

    ws.send(JSON.stringify({
      type: 'response.create',
      response: {
        output_modalities: ['audio'],
        ...payload,
      }
    }));
  }

  function flushResponseQueue() {
    if (!ws || ws.readyState !== ws.OPEN) return;
    if (isResponseActive) return;
    if (responseCreateQueue.length === 0) return;

    const now = Date.now();
    const diff = now - lastResponseCreateAt;

    const payload = responseCreateQueue.shift();

    if (diff >= RESPONSE_MIN_INTERVAL_MS) {
      _sendResponseCreateNow(payload);
    } else {
      setTimeout(() => {
        if (!isResponseActive) _sendResponseCreateNow(payload);
      }, RESPONSE_MIN_INTERVAL_MS - diff);
    }
  }

  function enqueueResponseCreate(payload = {}) {
    if (!ws || ws.readyState !== ws.OPEN) {
      responseCreateQueue.push(payload);
      return;
    }

    if (canSendNow()) {
      _sendResponseCreateNow(payload);
    } else {
      responseCreateQueue.push(payload);
    }
  }


  const processMessage = async (response) => {
    try {
      switch (response.type) {
        case 'session.created':
          logClient(`Session created for ${channelId}`);
          break;
        case 'session.updated':
          logOpenAI(`Session updated for ${channelId}`);
          break;
        case 'input_audio_buffer.speech_started':
          logOpenAI(`Client speech started for ${channelId}`);
          noteClientActivity();
          if (streamHandler) {
            try { streamHandler.stopPlayback(); } catch {}
          }
          break;
        case 'conversation.item.added':
          logOpenAI(`Conversation item added for ${channelId}`);
          if (response.item && response.item.id && response.item.role) {
            logger.debug(`Item created: id=${response.item.id}, role=${response.item.role} for ${channelId}`);
            itemRoles.set(response.item.id, response.item.role);
            if (response.item.role === 'user') {
              if (response.item.id === initialGreetingItemId) {
                logOpenAI(`Initial greeting item added for ${channelId}`);
              } else {
                lastUserItemId = response.item.id;
                noteClientActivity();
                logOpenAI(`User voice command detected for ${channelId}, stopping current playback`);
                logger.debug(`VAD triggered - Full message for user voice command: ${JSON.stringify(response, null, 2)}`);
                if (streamHandler) {
                  streamHandler.stopPlayback();
                }
              }
            }
          }
          break;
        case 'response.created':
          logOpenAI(`Response created for ${channelId}`);
          isResponseActive = true;
          break;
        
        case 'response.done': {
        isResponseActive = false;
        flushResponseQueue();
        const outputs = response?.response?.output || [];
        for (const out of outputs) {
          if (out.type === 'function_call') {
            const { name, call_id, arguments: args } = out;
            // --- ЕДИНАЯ АНТИ-ЗАЛИПАЛКА ДЛЯ РЕМОНТА ---
            const toolCfg = REPAIRABLE_TOOLS[name];
            if (toolCfg) {
              const ch = sipMap.get(channelId) || {};
              const curr = toolCfg.takeCurrent(args);
              const prev = toolCfg.takePreviousFromCh(ch);

              const retryCounters = ch.retryCounters || {};
              const retryCount = retryCounters[toolCfg.slot] || 0;

              // 5.1. если явная «битость» аргументов (пусто/слишком коротко) — не валидируем, а просим саморемонт
              if (toolCfg.looksInvalid && toolCfg.looksInvalid(curr) && retryCount > 0) {
                logger.warn(`[REPAIR] ${name}: looks invalid -> self-repair (retry=${retryCount})`);

                // 1) Сначала пишем function_call_output (без response.create)
                sendFunctionResult(ws, call_id, JSON.stringify({ ok: false, reason: 'invalid_args' }), enqueueResponseCreate, { createResponse: false });

                // 2) Потом self-repair (он и создаст ОДИН response.create через очередь)
                await triggerSelfRepair(ws, channelId, name, curr, 'looks_invalid', enqueueResponseCreate);

                continue;
              }

              if (toolCfg.isSame && toolCfg.isSame(prev, curr) && retryCount > 0) {
                logger.warn(`[REPAIR] ${name}: same args as previous -> self-repair (retry=${retryCount})`);

                sendFunctionResult(ws, call_id, JSON.stringify({ ok: false, reason: 'same_as_previous' }), enqueueResponseCreate, { createResponse: false });
                await triggerSelfRepair(ws, channelId, name, curr, 'same_args', enqueueResponseCreate);

                continue;
              }

              // 5.3. если всё ок — сохраняем «последние аргументы» для дальнейшего сравнения
              toolCfg.saveToCh(ch, curr);
              sipMap.set(channelId, ch);
            }
              let toolResult = null;

            const KNOWN_TOOLS = new Set(['validate_phone','validate_address','save_client_info','transfer_to_operator']);


            if (!REPAIRABLE_TOOLS[name] && !KNOWN_TOOLS.has(name)) {
              logger.warn(`[TOOLS] Unknown function_call: ${name}`);
              continue;
            }

            if (name === 'validate_phone') {
              let ch = sipMap.get(channelId) || {};
              const callerRaw = ch.callerNumber || '';
              const callerNorm = callerRaw ? (validateRussianPhone(callerRaw) || callerRaw) : '';

              logger.info(`[PHONE] validate_phone for ${channelId}: ${typeof args === 'string' ? args : JSON.stringify(args)}`);

              toolResult = await runValidatePhone(args);
              let parsed;
              try { parsed = JSON.parse(toolResult); } catch {}

              if (ch.slots) {
                ch.slots.phone.validated = true;
                ch.slots.phone.pendingTool = null;
              }

              if (parsed?.ok && parsed?.normalized) {
                ch.lastValidatedPhone = parsed.normalized;
                sipMap.set(channelId, ch);
                logger.info(`[PHONE] valid -> основной=${parsed.normalized} for ${channelId}`);
                sendFunctionResult(
                  ws,
                  call_id,
                  JSON.stringify({
                    ok: true,
                    normalized: parsed.normalized,
                    message: 'Номер принят как контактный. Не проговаривай его сейчас — сразу переходи к следующему пункту оформления.',
                  }),
                  enqueueResponseCreate
                );
              } else {
                ch.lastValidatedPhone = null;
                sipMap.set(channelId, ch);
                logger.info(`[PHONE] invalid -> откат на номер АТС ${callerNorm || callerRaw || '(нет)'} for ${channelId}`);
                sendFunctionResult(
                  ws,
                  call_id,
                  JSON.stringify({
                    ok: false,
                    fallback_to_caller: true,
                    caller_phone: callerNorm || callerRaw || '',
                    message: 'Номер не распознан. НЕ переспрашивай номер у клиента. Используй как контактный номер тот, с которого звонит клиент (caller_phone). Сразу переходи к следующему пункту оформления.',
                  }),
                  enqueueResponseCreate
                );
              }
            }
            else if (name === 'validate_address') {
              const a = (typeof args === 'string') ? JSON.parse(args) : (args || {});
              let ch = sipMap.get(channelId) || {};
              ch.retryCounters = ch.retryCounters || { phone: 0, address: 0 };

              if (a.direction) {
                ch.lastDirection = a.direction;
              }
              ch.retryCounters.address = (ch.retryCounters.address || 0) + 1;
              sipMap.set(channelId, ch);

              logger.info(`[ADDRESS] function_call attempt ${ch.retryCounters.address}: ${typeof args === 'string' ? args : JSON.stringify(args)}`);

              if (ch.retryCounters.address > config.MAX_VALIDATION_RETRIES) {
                logger.warn(`[ADDRESS] Max retries reached for ${channelId}, skipping validation`);
                enqueueResponseCreate({
                  output_modalities: ['audio'],
                  instructions: 'Не могу точно определить ваш адрес. Сейчас переведу вас на оператора, ожидайте, пожалуйста, на линии.'
                });
                ch.retryCounters.address = 0;
                sipMap.set(channelId, ch);

                toolResult = JSON.stringify({ ok: true, skipped: true });
                sendFunctionResult(ws, call_id, toolResult, enqueueResponseCreate, { createResponse: false });
              } else {
                if (!a.direction && ch.lastDirection) {
                  a.direction = ch.lastDirection;
                }

                toolResult = await runValidateAddress(a);

                let parsed = null;
                try { parsed = JSON.parse(toolResult); } catch {}

                const isOutOfService = parsed?.reason === 'out_of_service_zone';
                const isPaidZone = parsed?.ok === true && parsed?.transfer === true && parsed?.reason === 'paid_zone';

                if (isOutOfService || isPaidZone) {
                  ch = sipMap.get(channelId) || {};
                  ch.retryCounters = ch.retryCounters || { phone: 0, address: 0 };
                  ch.retryCounters.address = Math.max(0, (ch.retryCounters.address || 1) - 1);
                  sipMap.set(channelId, ch);
                }

                if (isOutOfService) {
                  logger.info(`[ADDRESS] zone=out_of_service for ${channelId} — оформление прекращается`);
                  sendFunctionResult(ws, call_id, toolResult, enqueueResponseCreate, { createResponse: false });
                  enqueueResponseCreate({
                    output_modalities: ['audio'],
                    instructions:
                      parsed.message ||
                      'Указанный клиентом адрес находится вне зоны работы нашей компании. Вежливо донесите это до клиента и прекратите далее оформлять заявку.',
                  });
                } else if (isPaidZone) {
                  logger.info(`[ADDRESS] zone=paid for ${channelId} — заявка на обратный звонок + завершение звонка`);
                  sendFunctionResult(ws, call_id, toolResult, enqueueResponseCreate, { createResponse: false });
                  requestCallbackTask();
                  const paidPhrase = isOperatorOffHours()
                    ? ('Адрес клиента вне зоны бесплатного выезда мастера. ' + OPERATOR_OFFHOURS_INSTRUCTION)
                    : 'Скажи клиенту ровно по смыслу: «К сожалению, ваш адрес находится вне зоны бесплатного выезда мастера. Оператор колл-центра перезвонит вам для уточнения условий. Спасибо за обращение, всего доброго!» и вежливо заверши разговор.';
                  enqueueResponseCreate({
                    output_modalities: ['audio'],
                    instructions: paidPhrase,
                  });
                  endCallAfterPhrase('PAID_ZONE');
                  continue;
                } else {
                  if (parsed?.ok === true && parsed?.normalized) {
                    ch = applyLastValidatedAddress(ch, parsed.normalized);
                    sipMap.set(channelId, ch);
                    logger.info(
                      `[ADDRESS] saved spoken_city="${ch.lastValidatedAddress.spoken_city}" ` +
                      `(system_city="${ch.lastValidatedAddress.system_city}") for ${channelId}`
                    );

                    const li = clientLocalDateInfo(ch.lastValidatedAddress?.timezone);
                    logger.info(`[ADDRESS] client local time for ${channelId}: ${li.todayISO} ${li.nowTime} (${li.zone}), withinWorkingHours=${li.withinWorkingHours}`);
                    const dateNote =
                      `[СИСТЕМНО] Местное время клиента: ${li.todayISO} ${li.nowTime} (${li.zone}). ` +
                      `Дату визита определяй ИМЕННО по этому местному времени клиента. ` +
                      `По умолчанию оформляй на сегодня (${li.todayISO}). ` +
                      (li.withinWorkingHours
                        ? ''
                        : `Сейчас у клиента наше нерабочее время, поэтому оформляй на завтра (${li.tomorrowISO}) и скажи клиенту, что мастер увидит заявку утром с 8 часов.`) +
                      `Дату у клиента не спрашивай; используй названную им дату, только если он сам её назвал.`;
                    try {
                      ws.send(JSON.stringify({
                        type: 'conversation.item.create',
                        item: {
                          type: 'message',
                          role: 'user',
                          content: [{ type: 'input_text', text: dateNote }],
                        },
                      }));
                    } catch (e) {
                      logger.error(`[ADDRESS] failed to send local-time note for ${channelId}: ${e.message}`);
                    }
                  }
                  sendFunctionResult(ws, call_id, toolResult, enqueueResponseCreate, {
                    createResponse: parsed?.ok === true,
                  });
                }
              }
            }
            else if (name === 'transfer_to_operator') {
              const a = (typeof args === 'string') ? JSON.parse(args) : (args || {});
              const reason = String(a.reason || '').trim();

              logger.info(`[HANDOFF] tool transfer_to_operator called | channel=${channelId} | reason="${reason}"`);

              if (isOperatorOffHours()) {
                logger.info(`[HANDOFF] off-hours (МСК) — перевод на оператора пропущен, создаю заявку на обратный звонок | channel=${channelId}`);
                sendFunctionResult(
                  ws,
                  call_id,
                  JSON.stringify({ ok: true, handoff: 'skipped', reason: 'operator_off_hours' }),
                  enqueueResponseCreate,
                  { createResponse: false }
                );
                requestCallbackTask();
                enqueueResponseCreate({
                  output_modalities: ['audio'],
                  instructions: OPERATOR_OFFHOURS_INSTRUCTION
                });
                continue;
              }

              // 1) ставим флаг, чтобы cleanup не повесил трубку
              let ch = sipMap.get(channelId) || {};
              ch.handoffToOperator = true;
              sipMap.set(channelId, ch);

              // 2) гасим текущую речь модели/плейбек
              try { ws.send(JSON.stringify({ type: 'response.cancel' })); } catch {}
              try { if (ch.streamHandler) ch.streamHandler.stopPlayback(); } catch {}

              // 3) пробуем реально перевести в диалплан
              try {
                await handoffToOperator(channelId, { context: 'from-TRUNK-MSK-RUS', exten: '1200', priority: 1 });
                logger.info(`[HANDOFF] continueInDialplan OK | channel=${channelId} -> from-TRUNK-MSK-RUS,1200,1`);

                // 4) tool output (без response.create)
                sendFunctionResult(
                  ws,
                  call_id,
                  JSON.stringify({ ok: true, handoff: 'started', target: 'from-TRUNK-MSK-RUS/1200' }),
                  enqueueResponseCreate,
                  { createResponse: false }
                );

                // 5) теперь можно закрыть WS
                try { ws.close(); } catch {}
              } catch (e) {
                logger.error(`[HANDOFF] continueInDialplan FAILED | channel=${channelId} | ${e.message}`);

                // снимаем флаг, чтобы cleanup работал стандартно
                ch = sipMap.get(channelId) || {};
                ch.handoffToOperator = false;
                sipMap.set(channelId, ch);

                // tool output — неуспех
                sendFunctionResult(
                  ws,
                  call_id,
                  JSON.stringify({ ok: false, error: e.message }),
                  enqueueResponseCreate
                );

                // голосом говорим клиенту, что не получилось
                enqueueResponseCreate({
                  output_modalities: ['audio'],
                  instructions: 'Не получилось перевести на оператора. Давайте продолжим здесь: что нужно отремонтировать?'
                });
              }

              continue;
            }
            else if (name === 'save_client_info') {
              // args → это JSON с полями заявки (name, direction, phone, address, и т.д.)
              // запускаем ваш Python-скрипт и возвращаем номер заявки
              const a = (typeof args === 'string') ? JSON.parse(args) : args;

              // соберём clientData почти как в handleSaveClientInfo
              const channelEntry = Array.from(sipMap.entries()).find(([, data]) => data.ws === ws);
              const callerNumber = channelEntry ? channelEntry[1].callerNumber : null;
              const chSave = sipMap.get(channelId);
              const validatedAddr = chSave?.lastValidatedAddress;
              const callerNorm = callerNumber ? (validateRussianPhone(callerNumber) || callerNumber) : '';
              const hasOwnMain = !!chSave?.lastValidatedPhone;
              const mainPhone = String(chSave?.lastValidatedPhone || callerNorm || a.phone || '');
              const additionalPhone = hasOwnMain ? (callerNumber || '') : '';

              let visitDate = String(a.date || '').trim();
              if (!visitDate) {
                const li = clientLocalDateInfo(validatedAddr?.timezone);
                visitDate = li.withinWorkingHours ? li.todayISO : li.tomorrowISO;
                logger.info(`[SAVE] date not provided -> default by client tz ${li.zone}: ${visitDate} (withinWorkingHours=${li.withinWorkingHours})`);
              }

              const clientData = {
                name: a.name,
                direction: a.direction,
                circumstances: a.circumstances || '',
                brand: a.brand || '',
                phone: mainPhone,
                phone2: additionalPhone,
                address: {
                  // city — город филиала для 1С (name_components, маршрутизация)
                  city: a.address?.city,
                  // spoken_city — реальный НП из DaData для address.name и логов
                  spoken_city: validatedAddr?.spoken_city
                    || a.address?.spoken_city
                    || '',
                  street: a.address?.street,
                  house_number: a.address?.house_number,
                  apartment: a.address?.apartment || '',
                  entrance: a.address?.entrance || '',
                  floor: a.address?.floor || '',
                  intercom: a.address?.intercom || '',
                  latitude: a.address?.latitude,
                  longitude: a.address?.longitude
                },
                date: visitDate,
                comment: a.comment || '',
                multipleRequest: !!(a.multiple_request ?? a.multipleRequest ?? false),
                phoneBot: chSave?.phoneBot || ''
              };
              // Подтверждение делает модель по промпту (одна проверка) — здесь сохраняем напрямую.
              const ch = sipMap.get(channelId) || {};
              if (ch?.slots && (!ch.slots.phone.validated || !ch.slots.address.validated)) {
                logger.warn(`[CHECKLIST] Слот не подтверждён: phone=${ch?.slots?.phone?.validated}, address=${ch?.slots?.address?.validated}`);
              }

              logger.info(
                `[SAVE] Saving save_client_info for ${channelId}, phone=${clientData.phone}`
              );

              sendFunctionResult(
                ws,
                call_id,
                JSON.stringify({
                  ok: true,
                  status: 'processing',
                  message: 'Оформление начато. Сначала озвучена фраза ожидания, номер заявки будет назван отдельно.'
                }),
                enqueueResponseCreate,
                { createResponse: false }
              );
              enqueueResponseCreate({
                output_modalities: ['audio'],
                instructions:
                  'Скажи клиенту дословно, без каких-либо добавлений и изменений: ' +
                  '«Оставайтесь, пожалуйста, на линии — сейчас я оформлю заявку и назову ее номер».'
              });

              try {
                const orderNum = await runSaveClientInfo(clientData, logger);
                ws.send(JSON.stringify({
                  type: 'conversation.item.create',
                  item: {
                    type: 'message',
                    role: 'system',
                    content: [{
                      type: 'input_text',
                      text: `Заявка успешно принята и сохранена. Номер оформленной заявки: ${orderNum}. Озвучь его клиенту по одной цифре (но только часть с цифрами, без буквенной).`
                    }]
                  }
                }));
                enqueueResponseCreate({ output_modalities: ['audio'] });
              } catch (e) {
                logger.error(`[SAVE] runSaveClientInfo failed for ${channelId}: ${e?.message || e}`);
                ws.send(JSON.stringify({
                  type: 'conversation.item.create',
                  item: {
                    type: 'message',
                    role: 'system',
                    content: [{
                      type: 'input_text',
                      text: 'Заявка принята и сохранена. '
                        + 'Сообщи это клиенту. '
                        + 'Номер заявки НЕ называй и НЕ придумывай.'
                    }]
                  }
                }));
                enqueueResponseCreate({ output_modalities: ['audio'] });
              }
            }
            if (name === 'validate_address') {
              if (!toolResult) { 
                  logger.warn(`[POST-RESULT] ${name}: empty toolResult`);
                  continue;
                }
              try {
                const parsed = JSON.parse(toolResult);
                let ch = sipMap.get(channelId);
                const slot = 'address';

                // 1) Обновляем слоты
                if (ch?.slots) {
                  ch.slots[slot].pendingTool = null;
                  ch.slots[slot].validated = parsed.ok === true;
                  logger.info(`[LOCK] Slot ${slot} validated=${ch.slots[slot].validated} for ${channelId}`);
                }

                // 1.1) Запоминаем нормализованный адрес для последующей озвучки —
                //      нам нужен «реальный» spoken_city из DaData, чтобы в чек-листе
                //      произнести клиенту его город, а не наш филиальный.
                if (name === 'validate_address' && parsed.ok === true && parsed.normalized) {
                  ch = applyLastValidatedAddress(ch, parsed.normalized);
                  logger.info(
                    `[ADDRESS] saved spoken_city="${ch.lastValidatedAddress.spoken_city}" ` +
                    `(system_city="${ch.lastValidatedAddress.system_city}") for ${channelId}`
                  );
                }

                // 2) Счётчики ретраев + авто-ремонт
                const retry = (ch?.retryCounters?.[slot] || 0);

                if (parsed.ok === true) {
                  if (ch?.retryCounters) {
                    ch.retryCounters[slot] = 0; // сброс счётчика КОНКРЕТНОГО слота
                    if (parsed.skipped) {
                      logger.info(`[${slot.toUpperCase()}] Validation skipped (max retries) — retry counter reset for ${channelId}`);
                    } else {
                      logger.info(`[${slot.toUpperCase()}] ✅ Validation succeeded — retry counter reset for ${channelId}`);
                    }
                  }
                } else {
                  // неуспех -> если это не первая попытка — self-repair
                  if (retry > 0 && REPAIRABLE_TOOLS?.[name]) {
                    const prev = REPAIRABLE_TOOLS[name].takePreviousFromCh(ch);
                    logger.warn(`[REPAIR] ${name}(${slot}): tool returned ok:false on retry=${retry} -> self-repair`);
                    await triggerSelfRepair(ws, channelId, name, prev || {}, 'fail_result', enqueueResponseCreate);

                  }
                }

                sipMap.set(channelId, ch);
              } catch (e) {
                logger.error(`[POST-RESULT] ${name}: cannot parse tool result: ${e?.message || e}`);
              }
            }
          }
        }
        flushResponseQueue();
        break;
}
        case 'response.output_audio.delta':
          if (response.delta) {
            const deltaBuffer = Buffer.from(response.delta, 'base64');
            if (deltaBuffer.length === 0 || isMuLawSilence(deltaBuffer)) {
              logger.warn(
                `Dropping silent/empty audio delta for ${channelId}: ${deltaBuffer.length} bytes`
              );
              break;
            }
            totalDeltaBytes += deltaBuffer.length;
            channelData.totalDeltaBytes = totalDeltaBytes;
            sipMap.set(channelId, channelData);
            segmentCount++;
            if (totalDeltaBytes - loggedDeltaBytes >= 40000 || segmentCount >= 100) {
              logOpenAI(
                `Received audio delta for ${channelId}: ${deltaBuffer.length} bytes, total: ${totalDeltaBytes} bytes, estimated duration: ${(totalDeltaBytes / 8000).toFixed(2)}s`,
                'info'
              );
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
          }
          break;
        case 'response.output_audio_transcript.delta':
          if (response.delta) {
            logger.debug(`Transcript delta for ${channelId}: ${response.delta.trim()}`);
            logger.debug(`Full transcript delta message: ${JSON.stringify(response, null, 2)}`);
          }
          break;
          case 'response.output_audio_transcript.done':
            if (response.transcript) {
              const role = response.item_id && itemRoles.get(response.item_id)
                ? itemRoles.get(response.item_id)
                : (lastUserItemId ? 'User' : 'Assistant');
  
              logger.debug(`Transcript done - Full message: ${JSON.stringify(response, null, 2)}`);
              const text = response.transcript;
  
              if (role === 'Assistant') {
                logOpenAI(`Assistant transcription for ${channelId}: ${text}`, 'info');
                let ch = sipMap.get(channelId) || {};
                ch.transcriptWindow = ch.transcriptWindow || [];
                ch.transcriptWindow.push({ role: 'assistant', text, t: Date.now() });
                if (ch.transcriptWindow.length > 10) ch.transcriptWindow.shift();
                sipMap.set(channelId, ch);
  
                ch = sipMap.get(channelId);
                if (ch) {
                  const audioBytes  = ch.lastAudioBytes || 0;
                  const audioSec    = audioBytes / 8000;
                  const expectedSec = (text || '').trim().length / 18;
                  const COVERAGE    = 0.8;
                  const MIN_EXPECTED = 10.0;
                  const tooShort    = expectedSec >= MIN_EXPECTED && audioSec < expectedSec * COVERAGE;
                  const retried     = ch.audioRetryCount || 0;
  
                  if (tooShort && retried < 3) {
                    ch.audioRetryCount = retried + 1;
                    sipMap.set(channelId, ch);
  
                    logger.warn(
                      `[RE-SPEAK] Short audio (${audioSec.toFixed(2)}s) for ` +
                      `${expectedSec.toFixed(1)}s of transcript on ${channelId}, ` +
                      `requesting re-speak (attempt ${ch.audioRetryCount})`
                    );
  
                    enqueueResponseCreate({
                      output_modalities: ['audio'],
                      instructions:
                        'Произнеси вслух эту реплику в точности, без изменений и без добавлений, нормальной речью: ' +
                        `"${text.replace(/"/g, '\\"')}"`
                    });
                    break;
                  }
  
                  if (!tooShort && retried) {
                    ch.audioRetryCount = 0;
                    sipMap.set(channelId, ch);
                  }
                }
  
                ch = sipMap.get(channelId);
                if (ch?.slots) {
                  const tryingToSkip = /\bпер(е|е)йд(е|ё)м|дал(е|ё)е\b/i.test(text); // «перейдём», «далее»
                  const unconfirmed = (!ch.slots.phone.validated || !ch.slots.address.validated);
  
                  if (tryingToSkip && unconfirmed) {
                    try { ws.send(JSON.stringify({ type: 'response.cancel' })); } catch {}
                    const target = !ch.slots.phone.validated ? 'phone' : 'address';
                    const nudge =
                      target === 'phone'
                        ? 'Прежде чем перейти дальше, пожалуйста, продиктуйте номер ещё раз полностью, сейчас именно по одной цифре, начиная с +7 — и я проверю.'
                        : 'Прежде чем перейти дальше, давайте уточним адрес: город, улица, дом — я проверю и повторю итог.';
  
                    enqueueResponseCreate({
                      output_modalities: ['audio'],
                      instructions: nudge
                    });
                  }
                }
              } else {
                logOpenAI(`User command transcription for ${channelId}: ${text}`, 'info');
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
            const text = response.transcript;
            logOpenAI(`User command transcription for ${channelId}: ${text}`, 'info');
            // --- сохраняем реплику пользователя в историю транскриптов канала ---
            let ch = sipMap.get(channelId) || {};
            ch.transcriptWindow = ch.transcriptWindow || [];
            ch.transcriptWindow.push({ role: 'user', text, t: Date.now() });
            if (ch.transcriptWindow.length > 10) ch.transcriptWindow.shift();
            if (!ch.lastDirection) {
              const earlyDir = detectDirection(text);
              if (earlyDir) {
                ch.lastDirection = earlyDir;
                logger.info(`[DIRECTION] раннее определение "${earlyDir}" из речи клиента для ${channelId}`);
              }
            }
            sipMap.set(channelId, ch);

            ch = sipMap.get(channelId);
            if (ch?.slots && ch.slots.address.validated && hasCorrectionIntent(text)) {
              const mentionsPhone = /телефон|номер/i.test(text);
              const mentionsAddr = /адрес|город|улиц|дом|кварт|подъезд|этаж|домофон/i.test(text);
              const target = (mentionsPhone && !mentionsAddr) ? 'phone'
                           : (mentionsAddr && !mentionsPhone) ? 'address'
                           : null;

              if (target === 'address') {
                ch.slots.address.validated = false;
                ch.slots.address.pendingTool = null;
                sipMap.set(channelId, ch);
                try { ws.send(JSON.stringify({ type: 'response.cancel' })); } catch {}
                enqueueResponseCreate({
                  output_modalities: ['audio'],
                  instructions: 'Давайте ещё раз: город, улица, дом — продиктуйте, пожалуйста, полностью. Я проверю адрес и повторю вам итог.'
                });
              } else if (target === 'phone') {
                try { ws.send(JSON.stringify({ type: 'response.cancel' })); } catch {}
                enqueueResponseCreate({
                  output_modalities: ['audio'],
                  instructions: 'Хорошо, продиктуйте, пожалуйста, контактный номер полностью, сейчас именно по одной цифре, начиная с +7 — я проверю и сразу повторю вам то, что записал.'
                });
              }
            }
          }
          break;
          case 'response.output_audio.done': {
            const audioSec = totalDeltaBytes / 8000;
            logOpenAI(
              `Response audio done for ${channelId}, total delta bytes: ${totalDeltaBytes}, estimated duration: ${audioSec.toFixed(2)}s`,
              'info'
            );

            if (channelData) {
              channelData.lastAudioBytes = totalDeltaBytes;
              channelData.lastAudioAt = Date.now();
            }

            isResponseActive = false;
            loggedDeltaBytes = 0;
            segmentCount = 0;
            totalDeltaBytes = 0;
            if (channelData) channelData.totalDeltaBytes = 0;
            itemRoles.clear();
            lastUserItemId = null;
            responseBuffer = Buffer.alloc(0);
            flushResponseQueue();
            scheduleInactivityAfterSpeech();
            break;
          }
        case 'error': {
          const msg = response?.error?.message || '';
          logger.error(`OpenAI error for ${channelId}: ${msg}`);

          if (msg.includes('Conversation already has an active response')) {
            // считаем, что ответ всё ещё активен, и просто подождём done/audio.done
            isResponseActive = true;

            // на всякий случай попробуем позже дёрнуть очередь
            setTimeout(() => {
              if (!isResponseActive) flushResponseQueue();
            }, 300);

            break;
          }

          if (msg.includes('Cancellation failed') || msg.includes('no active response')) {
            isResponseActive = false;
            flushResponseQueue();
            break;
          }

          ws.close();
          break;
        }
        case "conversation.item.input_audio_transcription.delta":
          logger.debug(`[OpenAI] user speech (partial) for ${channelId}: ${response.delta}`);
          break;

        case "conversation.item.input_audio_transcription.completed":
          const text = response.transcript;
          logOpenAI(`User transcription for ${channelId}: ${text}`);
    
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
          'Authorization': `Bearer ${OPENAI_API_KEY}`
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
        direction: {
          type: 'string', description: 'цель / причина обращения',
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
            'Клининг',
            'Дезинсекция',
            'Натяжные потолки',
            'Мелкобытовой сервис',
            'Ремонт квартир',
            'Сантехника',
            'Вывоз мусора',
            'Уборка',
            'Электрика',
            'Вскрытие и установка замков',
            'Окна'
          ]
        },
        circumstances:{ type: 'string', description: 'Подробности неисправности / обращения' },
        brand:       { type: 'string',  description: 'Бренд / модель - если техника, одной строкой' },
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
            city:        { type: 'string', description: 'Город филиала из validate_address.normalized.city (для системы). Не подставляй сюда spoken_city.' },
            spoken_city: { type: 'string', description: 'Реальный населённый пункт из validate_address.normalized.spoken_city (для address.name в 1С). Если не передаёшь — подставится из последней валидации.' },
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
        date:   { type: 'string', description: 'Желаемая дата визита (YYYY-MM-DD). Конвертируй относительные фразы на основе текущей даты из системного сообщения.', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
        comment: { type: 'string', description: 'Дополнительный комментарий' },
        multiple_request: { type: 'boolean', default: false, description: 'True, если клиент говорил о нескольких поломках/единицах ТЕХНИКИ (в том числе в направлении Установка) именно при одинаковом направлении для нескольких отдельных заявок, и False, если есть только единичная заявка для одной единицы техники, или же клиент говорил о разных запросах в рамках одного направления именно уже УСЛУГ' }
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
    description: 'Проверяет адрес, рассчитывает удалённость от ближайшего филиала компании и зону обслуживания. Возвращает нормализованный адрес (в normalized: city — город филиала для save_client_info), street, house_number, координаты и решение по зоне (ok / paid_zone / out_of_service_zone).',
    parameters: {
      type: 'object',
      required: ['city','street','house_number'],
      properties: {
        city:         { type: 'string', description: 'Город' },
        street:       { type: 'string', description: 'Улица' },
        house_number: { type: 'string', description: 'Дом / корпус / строение' },
        direction: {
          type: 'string',
          description: 'Направление заявки, как в save_client_info.direction. Передавай ровно то направление, которое клиент назвал в начале разговора. Нужно для определения зоны выезда мастера.',
          enum: [
            'Холодильники', 'Кондиционеры', 'Телевизоры', 'Стиральные машины',
            'Посудомоечные машины','Швейные машины','Кофемашины','Плиты',
            'Микроволновки','Вытяжки','Компьютеры','Гаджеты','Промышленный холод',
            'Газовые колонки','Установка','Клининг','Дезинсекция',
            'Натяжные потолки','Мелкобытовой сервис','Ремонт квартир','Сантехника',
            'Вывоз мусора','Уборка','Электрика','Окна','Вскрытие и установка замков'
          ]
        }
      }
    }
  },
{
  type: 'function',
  name: 'transfer_to_operator',
  description: 'Переводит звонок на оператора (в очередь/КЦ). Вызывать, когда по правилам SYSTEM_PROMPT требуется перевод на оператора. После вызова прекрати диалог.',
  parameters: {
    type: 'object',
    required: ['reason'],
    properties: {
      reason: {
        type: 'string',
        description: 'Одна причина из списка причин перевода из SYSTEM_PROMPT.'
      }
    }
  }
}
];

      ws.on('open', async () => {
        logClient(`OpenAI WebSocket connected for ${channelId}`);
        const systemPromptFinal = buildSystemPromptFinal();

        const callerRaw = channelData.callerNumber || '';
        const callerNorm = callerRaw ? (validateRussianPhone(callerRaw) || callerRaw) : '';
        const phoneStepNote = callerNorm
          ? `\n\n[ТЕЛЕФОН] Номер, с которого звонит клиент (АТС): ${callerNorm}. Это контактный номер по умолчанию. На этапе телефона (пункт 5) НЕ проси клиента именно диктовать номер — спроси своими словами, можно ли использовать для связи с мастером именно тот номер, с которого он звонит. Если согласен — используй ${callerNorm} как контактный и НЕ вызывай validate_phone. Если хочет другой номер — попроси продиктовать и вызови validate_phone один раз.`
          : `\n\n[ТЕЛЕФОН] Номер АТС неизвестен. На этапе телефона (пункт 5) попроси клиента продиктовать контактный номер полностью, начиная с +7, и вызови validate_phone.`;
        const sessionInstructions = systemPromptFinal + phoneStepNote;
        logClient(`Caller number for ${channelId}: raw="${callerRaw}" norm="${callerNorm}"`);

        ws.send(JSON.stringify({
          type: 'session.update',
          session: {
            type: 'realtime',
            instructions: sessionInstructions,
            output_modalities: ['audio'],
            audio: {
              input: {
                format: { type: 'audio/pcmu' },
                transcription: {
                  model: 'gpt-4o-transcribe',
                  language: 'ru'
                },
                turn_detection: {
                  type: 'server_vad',
                  threshold: config.VAD_THRESHOLD || 0.6,
                  prefix_padding_ms: config.VAD_PREFIX_PADDING_MS || 200,
                  silence_duration_ms: config.VAD_SILENCE_DURATION_MS || 600,
                  create_response: true
                }
              },
              output: {
                format: { type: 'audio/pcmu' },
                voice: config.OPENAI_VOICE || 'cedar'
              }
            },
            include: ["item.input_audio_transcription.logprobs"],
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
          channelData.retryCounters = { phone: 0, address: 0 };
          channelData.slots = {
          phone:   { required: true, validated: !!callerNorm, pendingTool: null },
          address: { required: true, validated: false, pendingTool: null },
          };
          sipMap.set(channelId, channelData);

          const itemId = uuid().replace(/-/g, '').substring(0, 32);
          initialGreetingItemId = itemId;
          logClient(`Sending initial message for ${channelId}: ${config.INITIAL_MESSAGE || 'Здравствуйте! Я голосовой помощник сервисного центра. Оформляю заявку на выезд мастера'}`);
          ws.send(JSON.stringify({
            type: 'conversation.item.create',
            item: {
              id: itemId,
              type: 'message',
              role: 'user',
              content: [{ type: 'input_text', text: config.INITIAL_MESSAGE || 'Здравствуйте! Я голосовой помощник сервисного центра. Оформляю заявку на выезд мастера' }]
            }
          }));
            enqueueResponseCreate({
                output_modalities: ['audio'],
                instructions: sessionInstructions
              });
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
        clearInactivityTimer();
        clearPlaybackFallback();
        rtpEvents.off('audioFinished', onAudioFinishedForSilence);
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
