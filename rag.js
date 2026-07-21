const { config, logger } = require('./config');

const RAG_URL = (config.RAG_URL || process.env.RAG_URL || 'http://127.0.0.1:7420').replace(/\/$/, '');
const RAG_ENABLED = String(config.RAG_ENABLED ?? process.env.RAG_ENABLED ?? 'true').toLowerCase() === 'true';
const RAG_TIMEOUT_MS = Number(config.RAG_TIMEOUT_MS || process.env.RAG_TIMEOUT_MS || 5000);

const RAG_BLOCK_HEADER =
  '### Дополнительные инструкции, которые помогут точнее сориентироваться в обращении клиента:';

const INSTRUCTIONS_PREVIEW_CHARS = Number(process.env.RAG_INSTRUCTIONS_PREVIEW_CHARS || 400);

function previewText(text, maxChars = INSTRUCTIONS_PREVIEW_CHARS) {
  const s = String(text || '').replace(/\s+/g, ' ').trim();
  if (!s) return '(empty)';
  if (s.length <= maxChars) return s;
  return `${s.slice(0, maxChars)}…[+${s.length - maxChars} chars]`;
}

/**
 * Запрос к RAG sidecar.
 * @param {string} query
 * @returns {Promise<string>} context или ''
 */
async function fetchRagContext(query) {
  if (!RAG_ENABLED) return '';
  const q = String(query || '').trim();
  if (!q) return '';

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), RAG_TIMEOUT_MS);
  try {
    const resp = await fetch(`${RAG_URL}/search`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ query: q, k_vector: 6, k_bm25: 6, top_k: 6 }),
      signal: ctrl.signal,
    });
    if (!resp.ok) {
      logger.warn(`[RAG] HTTP ${resp.status} from ${RAG_URL}/search`);
      return '';
    }
    const json = await resp.json();
    const context = String(json?.context || '').trim();
    if (context) {
      logger.info(`[RAG] context chars=${context.length} for query="${q.slice(0, 80)}"`);
      logger.info(`[RAG] context preview: ${previewText(context)}`);
    } else {
      logger.info(`[RAG] empty context for query="${q.slice(0, 80)}"`);
    }
    return context;
  } catch (e) {
    const msg = e?.name === 'AbortError' ? `timeout ${RAG_TIMEOUT_MS}ms` : e.message;
    logger.warn(`[RAG] search failed: ${msg}`);
    return '';
  } finally {
    clearTimeout(timer);
  }
}

function buildInstructionsWithRag(baseInstructions, ragContext) {
  const base = String(baseInstructions || '');
  const ctx = String(ragContext || '').trim();
  if (!ctx) return base;
  return `${base}\n\n${RAG_BLOCK_HEADER}\n${ctx}`;
}

/**
 * Дождаться RAG и собрать instructions для ТЕКУЩЕГО response.create.
 * При ошибке/таймауте возвращает base без RAG.
 */
async function buildTurnInstructions({ baseInstructions, query }) {
  const base = String(baseInstructions || '');
  if (!RAG_ENABLED) return { instructions: base, ragContext: '' };
  const ragContext = await fetchRagContext(query);
  const instructions = buildInstructionsWithRag(base, ragContext);
  return { instructions, ragContext };
}

module.exports = {
  RAG_ENABLED,
  fetchRagContext,
  buildInstructionsWithRag,
  buildTurnInstructions,
  previewText,
};
