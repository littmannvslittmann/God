'use strict';

/**
 * Deriv EMA Indicator — Node.js (server-side)
 * Requires: ws  →  npm install ws
 * Usage:    node indicators.js
 */

const WebSocket = require('ws');

// ─── Telegram configuration ───────────────────────────────────────────────────
const TELEGRAM_BOT_TOKEN = '8809509406:AAEOH2jrzNJNkrDPSh0EvN5pAO9qdMuJpB8';
const TELEGRAM_CHAT_ID   = '6456659526';

// ─── Deriv WebSocket ──────────────────────────────────────────────────────────
// NOTE: This is the public endpoint for Deriv's Options Trading API, not the
// classic API this script's ticks_history/candles/ticks messages target.
// If candles/ticks stop coming through, that's likely why.
const API_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';
let ws;

// ─── Indicator Configuration ──────────────────────────────────────────────────
const COOLDOWN_CANDLES   = 4;        // Closed candles to wait before re-alerting for touches

// ─── EMA configuration ─────────────────────────────────────────────────────────
const EMA_PERIODS        = [20, 100];   // Classic EMAs
const TOUCH_ALERT_EMAS   = [20, 100];   // EMAs used to detect a "touch" and fire the touch alert

// Emoji mapping to identify the EMA period
const EMA_EMOJIS = {
  20:  '2️⃣0️⃣',
  100: '1️⃣0️⃣0️⃣'
};

// ─── EMA-pullback candle-streak configuration ──────────────────────────────────
const PULLBACK_EMA_PERIOD   = 20;      // EMA used to gauge trend side for the pullback alert
const PULLBACK_STREAK_LEN   = 2;       // Consecutive pullback candles required to fire
const PULLBACK_EMOJIS = {
  redPullback:   '🔴',   // uptrend (price above EMA20): red candles that still close at/above it
  greenPullback: '🟢'    // downtrend (price below EMA20): green candles that still close at/below it
};

// ─── Consecutive same-colour candle configuration ──────────────────────────────
// Independent of EMA and of the pullback streak: fires whenever N closed
// candles in a row share the same colour, anywhere on the chart.
const CONSECUTIVE_CANDLE_LEN = 4;      // Same-colour candles in a row required to fire
const CONSECUTIVE_EMOJIS = {
  bullish: '🟢',
  bearish: '🔴'
};

// ─── Symbols & timeframes ─────────────────────────────────────────────────────
const SYMBOLS    = ['R_10' /*, 'R_25' */];
const TIMEFRAMES = ['30min'];

const timeframeMap = { '30min': 1800 }; // 15 mins = 900 seconds

const displayNames = {
  'R_10':    'Volatility 10 Index',
  // 'R_25':    'Volatility 25 Index',
  '30min':  '30 minutes'
};

const MAX_HISTORICAL_CANDLES = 5000;

// ─── State ───────────────────────────────────────────────────────────────────
const historicalData        = {};
const currentCandles        = {};
const emaNotificationState  = {};
const emaState              = {};
const streakState           = {};
const candleRunState        = {};  // same-colour candle run tracker, per symbol/timeframe

function initState() {
  SYMBOLS.forEach(sym => {
    historicalData[sym]        = {};
    currentCandles[sym]        = {};
    emaNotificationState[sym]  = {};
    emaState[sym]               = {};
    streakState[sym]           = {};
    candleRunState[sym]        = {};

    TIMEFRAMES.forEach(tf => {
      historicalData[sym][tf]        = [];
      currentCandles[sym][tf]        = null;
      emaNotificationState[sym][tf]  = {};
      streakState[sym][tf]           = { type: null, count: 0, alertSent: false };
      candleRunState[sym][tf]        = { color: null, count: 0, alertSent: false };

      EMA_PERIODS.forEach(period => {
        emaNotificationState[sym][tf][period] = {
          lastAlertTimestamp: null,
          notifSent: false
        };
      });
    });

    EMA_PERIODS.forEach(period => {
      emaState[sym][period] = null;
    });
  });
}

initState();

// ─── Telegram ────────────────────────────────────────────────────────────────
async function sendTelegramNotification(message, dedupKey) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.error('[Telegram] NOT SENT - TELEGRAM_BOT_TOKEN is not set. Alert was:', message);
    return;
  }

  const url  = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  const body = JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'Markdown' });

  try {
    const res  = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
    const data = await res.json();
    if (!data.ok) console.error('[Telegram] API error:', data);
    else          console.log(`[Telegram] Sent ✓ (${dedupKey})`);
  } catch (err) {
    console.error('[Telegram] Send failed:', err.message);
  }
}

// ─── Alert batching ────────────────────────────────────────────────────────────
// Candle-close alerts (EMA touch, pullback streak, consecutive candles)
// are collected while a candle closes and sent as ONE message, e.g.
//   "🟣 🔴🔴🔴 Volatility 10 Index"
let alertBatch = null;

function startAlertBatch() { alertBatch = []; }

function queueAlert(emoji, symbolName, dedupKey) {
  if (alertBatch) alertBatch.push({ emoji, symbolName, dedupKey });
  else {
    const message = `${emoji} ${symbolName}`;
    console.log(`\n${message}`);
    sendTelegramNotification(message, dedupKey);
  }
}

function flushAlertBatch() {
  const batch = alertBatch;
  alertBatch = null;
  if (!batch || batch.length === 0) return;

  const message = `${batch.map(a => a.emoji).join(' ')} ${batch[0].symbolName}`;
  const keys    = batch.map(a => a.dedupKey).join(' + ');
  console.log(`\n${message}`);
  sendTelegramNotification(message, keys);
}

// ─── EMA helpers ──────────────────────────────────────────────────────────────
function initEMA(symbol, closedCandles, period) {
  const data = closedCandles
    .filter(c => isFinite(c.close) && c.close > 0)
    .sort((a, b) => a.timestamp - b.timestamp);

  if (data.length < period) { emaState[symbol][period] = null; return; }

  let ema = 0;
  for (let i = 0; i < period; i++) ema += data[i].close;
  ema /= period;

  const k = 2 / (period + 1);
  for (let i = period; i < data.length; i++) {
    ema = data[i].close * k + ema * (1 - k);
  }
  emaState[symbol][period] = ema;
}

function advanceEMA(symbol, period, closedClose) {
  if (emaState[symbol][period] === null) return;
  const k = 2 / (period + 1);
  emaState[symbol][period] = closedClose * k + emaState[symbol][period] * (1 - k);
}

function getEMA(symbol, period) {
  return emaState[symbol][period];
}

// ─── EMA touch detection ───────────────────────────────────────────────────────
// Fires a touch alert for each EMA listed in TOUCH_ALERT_EMAS (EMA20 and EMA100).
function checkEMATouches(symbol, timeframe, closedCandle) {
  const symbolName  = displayNames[symbol] || symbol;
  const granularity = timeframeMap[timeframe];
  const currentTimestamp = closedCandle.timestamp;

  EMA_PERIODS.forEach(period => {
    const ema = getEMA(symbol, period);
    if (ema === null) return;

    // Touch condition: The EMA value lies anywhere between or exactly on the Candle's High and Low
    const touched = closedCandle.low <= ema && closedCandle.high >= ema;

    const dedupKey = `${symbol}:${timeframe}:ema${period}`;
    const state    = emaNotificationState[symbol][timeframe][period];
    const emaEmoji = EMA_EMOJIS[period] || period;

    // Process EMA Touch — only for the designated touch-alert EMAs
    if (TOUCH_ALERT_EMAS.includes(period) && touched) {
      // Evaluate Cooldown for Touch Alert (same COOLDOWN_CANDLES for every EMA)
      const candlesPassed = state.lastAlertTimestamp === null
        ? Infinity
        : (currentTimestamp - state.lastAlertTimestamp) / granularity;

      const candlesClear = candlesPassed >= COOLDOWN_CANDLES;

      if (state.notifSent && candlesClear) {
        state.notifSent = false;
        console.log(`[Lock] Released ${dedupKey}`);
      }

      if (!state.notifSent) {
        state.lastAlertTimestamp = currentTimestamp;
        state.notifSent          = true;

        queueAlert(emaEmoji, symbolName, dedupKey);
      }
    }
  });
}

// ─── Candle color + EMA(20) pullback streak detection ───────────────────────────
// Candle colour is decided purely by open vs close:
//   close > open  -> bullish
//   close < open  -> bearish
//   close === open -> no colour (doji); this resets any running streak
function classifyCandleColor(symbol, timeframe, closedCandle) {
  if (closedCandle.close > closedCandle.open) return 'bullish';
  if (closedCandle.close < closedCandle.open) return 'bearish';
  return null;
}

function classifyPullbackType(symbol, timeframe, closedCandle) {
  const color = classifyCandleColor(symbol, timeframe, closedCandle);
  const ema   = getEMA(symbol, PULLBACK_EMA_PERIOD);
  if (color === null || ema === null) return null;

  // Uptrend pullback: a red candle that still closes at/above the EMA(20)
  if (color === 'bearish' && closedCandle.close >= ema) return 'redPullback';

  // Downtrend pullback (vice versa): a green candle that still closes at/below the EMA(20)
  if (color === 'bullish' && closedCandle.close <= ema) return 'greenPullback';

  return null;
}

function checkEmaPullbackStreak(symbol, timeframe, closedCandle) {
  const symbolName = displayNames[symbol] || symbol;
  const type        = classifyPullbackType(symbol, timeframe, closedCandle);
  const state        = streakState[symbol][timeframe];

  if (type && type === state.type) {
    state.count += 1;
  } else {
    // Streak broke (pattern changed, or candle doesn't qualify) — start over and
    // unlock the alert so a fresh streak of PULLBACK_STREAK_LEN can fire again.
    state.type       = type;
    state.count      = type ? 1 : 0;
    state.alertSent  = false;
  }

  if (state.count === PULLBACK_STREAK_LEN && !state.alertSent) {
    state.alertSent = true;

    const dedupKey = `${symbol}:${timeframe}:pullback`;
    const emoji    = PULLBACK_EMOJIS[type].repeat(PULLBACK_STREAK_LEN);
    queueAlert(emoji, symbolName, dedupKey);
  }
}

// ─── Consecutive same-colour candle detection ──────────────────────────────────
// Fires once when CONSECUTIVE_CANDLE_LEN closed candles in a row are the same
// colour (all bullish or all bearish), regardless of where they sit vs the EMAs.
function checkConsecutiveCandles(symbol, timeframe, closedCandle) {
  const symbolName = displayNames[symbol] || symbol;
  const color      = classifyCandleColor(symbol, timeframe, closedCandle);
  const state      = candleRunState[symbol][timeframe];

  if (color && color === state.color) {
    state.count += 1;
  } else {
    // Run broke (colour changed, or colourless candle) — start over and unlock the alert
    state.color     = color;
    state.count     = color ? 1 : 0;
    state.alertSent = false;
  }

  if (state.count === CONSECUTIVE_CANDLE_LEN && !state.alertSent) {
    state.alertSent = true;

    const dedupKey = `${symbol}:${timeframe}:run${CONSECUTIVE_CANDLE_LEN}`;
    const emoji    = CONSECUTIVE_EMOJIS[color].repeat(CONSECUTIVE_CANDLE_LEN);
    queueAlert(emoji, symbolName, dedupKey);
  }
}

// Rebuild the same-colour run from the API's CLOSED candles (what the chart shows),
// so the counter is correct after a start/reconnect instead of carrying stale state.
// A run that already exists is marked as alerted so it doesn't fire retroactively.
function seedRunState(symbol, timeframe, closedCandles) {
  const st = candleRunState[symbol][timeframe];
  st.color = null; st.count = 0; st.alertSent = false;
  closedCandles.forEach(c => {
    const color = classifyCandleColor(symbol, timeframe, c);
    if (color && color === st.color) st.count += 1;
    else { st.color = color; st.count = color ? 1 : 0; }
  });
  st.alertSent = st.count >= CONSECUTIVE_CANDLE_LEN;

  // Pullback streak needs the EMA at each historical bar, so just start it clean.
  streakState[symbol][timeframe] = { type: null, count: 0, alertSent: false };
}

// ─── Candle management ───────────────────────────────────────────────────────
function getCandleTimeframe(timestamp, granularity) {
  return Math.floor(timestamp / granularity) * granularity;
}

function updateCurrentCandle(symbol, price, timestamp) {
  Object.keys(timeframeMap).forEach(timeframe => {
    const granularity = timeframeMap[timeframe];
    const candleTime  = getCandleTimeframe(timestamp, granularity);

    // A late tick from an already-closed candle must never open a "new" candle:
    // that closed the live candle early and corrupted open/colour of the next one.
    const live = currentCandles[symbol][timeframe];
    if (live && candleTime < live.timestamp) return;

    if (!currentCandles[symbol][timeframe] ||
        currentCandles[symbol][timeframe].timestamp !== candleTime) {

      if (currentCandles[symbol][timeframe]) {
        const closedCandle = currentCandles[symbol][timeframe];
        historicalData[symbol][timeframe].push(closedCandle);
        if (historicalData[symbol][timeframe].length > MAX_HISTORICAL_CANDLES)
          historicalData[symbol][timeframe].shift();

        const closedClose = closedCandle.close;

        // Advance the EMAs with the closed candle's close FIRST, so every check below
        // compares against the same value a chart shows on that candle. Previously the
        // checks ran against the PREVIOUS candle's EMA, which caused wrong alerts.
        EMA_PERIODS.forEach(period => {
          advanceEMA(symbol, period, closedClose);
        });

        startAlertBatch();

        // Check for EMA(20) and EMA(100) touches using the fully formed closed candle
        checkEMATouches(symbol, timeframe, closedCandle);

        // Check for EMA(20) pullback candle streaks using the fully formed closed candle
        checkEmaPullbackStreak(symbol, timeframe, closedCandle);

        // Check for consecutive same-colour candles using the fully formed closed candle
        checkConsecutiveCandles(symbol, timeframe, closedCandle);

        flushAlertBatch();   // one combined message for everything that fired on this close

        const rs = candleRunState[symbol][timeframe];
        console.log(
          `\n[${symbol}/${timeframe}] CLOSED ${new Date(closedCandle.timestamp * 1000).toISOString()} ` +
          `O:${closedCandle.open} H:${closedCandle.high} L:${closedCandle.low} C:${closedCandle.close} ` +
          `| colour:${classifyCandleColor(symbol, timeframe, closedCandle)} | run:${rs.color} x${rs.count}`
        );
      }

      currentCandles[symbol][timeframe] = {
        timestamp: candleTime, open: price, high: price, low: price, close: price
      };
    } else {
      const c = currentCandles[symbol][timeframe];
      c.high  = Math.max(c.high, price);
      c.low   = Math.min(c.low,  price);
      c.close = price;
    }
  });
}

// ─── Indicator recalculation ─────────────────────────────────────────────────
function recalculateIndicators(symbol, timeframe, livePrice) {
  if (!historicalData[symbol][timeframe].length || !currentCandles[symbol][timeframe]) return;

  let emaString = '';
  EMA_PERIODS.forEach(period => {
    const emaVal = getEMA(symbol, period);
    emaString += `EMA${period}:${emaVal !== null ? emaVal.toFixed(4) : 'N/A'} `;
  });

  process.stdout.write(
    `\r[${symbol}] Price:${livePrice.toFixed(4)} ${emaString}  `
  );
}

// ─── Historical candle processing ────────────────────────────────────────────
function processCandles(symbol, timeframe, candles) {
  const data = candles
    .map(c => ({ open: parseFloat(c.open), high: parseFloat(c.high),
                 low: parseFloat(c.low), close: parseFloat(c.close), timestamp: c.epoch }))
    .filter(c => isFinite(c.close) && c.close > 0)
    .sort((a, b) => a.timestamp - b.timestamp);

  if (!data.length) return;

  historicalData[symbol][timeframe] = data.slice(0, -1);

  const lastCandle = data[data.length - 1];
  currentCandles[symbol][timeframe] = {
    timestamp: lastCandle.timestamp,
    open: lastCandle.open, high: lastCandle.high,
    low: lastCandle.low,   close: lastCandle.close
  };

  seedRunState(symbol, timeframe, historicalData[symbol][timeframe]);

  EMA_PERIODS.forEach(period => {
    initEMA(symbol, historicalData[symbol][timeframe], period);
  });

  const emaLogDetails  = EMA_PERIODS.map(p => `EMA${p}:${emaState[symbol][p]?.toFixed(4) ?? 'N/A'}`).join(' | ');
  console.log(
    `[${symbol}/${timeframe}] Loaded ${data.length} candles | ${emaLogDetails}`
  );
}

// ─── WebSocket ───────────────────────────────────────────────────────────────
function sendMessage(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function requestCandles(symbol, timeframe) {
  sendMessage({
    ticks_history: symbol, adjust_start_time: 1,
    count: MAX_HISTORICAL_CANDLES, end: 'latest',
    style: 'candles', granularity: timeframeMap[timeframe]
  });
}

// ─── WebSocket initialization & event handling ───────────────────────────────
function subscribeToTicks(symbol) {
  sendMessage({ ticks: symbol, subscribe: 1 });
}

const lastTickEpoch = {};

function handleMessage(raw) {
  let data;
  try { data = JSON.parse(raw); } catch { return; }

  if (data.error) { console.error('[WS] Error:', data.error.message); return; }

  if (data.candles) {
    const symbol    = data.echo_req.ticks_history;
    const tf        = Object.keys(timeframeMap).find(k => timeframeMap[k] === data.echo_req.granularity);
    if (tf) processCandles(symbol, tf, data.candles);
  }

  if (data.tick) {
    const { symbol, quote, epoch } = data.tick;
    const price = parseFloat(quote);

    if (lastTickEpoch[symbol] === epoch) return;
    lastTickEpoch[symbol] = epoch;

    updateCurrentCandle(symbol, price, epoch);
    Object.keys(timeframeMap).forEach(tf => recalculateIndicators(symbol, tf, price));
  }
}

function initializeWebSocket() {
  console.log('[WS] Connecting…');
  ws = new WebSocket(API_URL);

  ws.on('open', () => {
    console.log('[WS] Connected');
    SYMBOLS.forEach(sym => {
      TIMEFRAMES.forEach(tf => requestCandles(sym, tf));
      subscribeToTicks(sym);
    });
  });

  ws.on('message', handleMessage);

  ws.on('close', () => {
    console.log('\n[WS] Disconnected — reconnecting in 5s…');
    setTimeout(initializeWebSocket, 5_000);
  });

  ws.on('error', err => console.error('[WS] Error:', err.message));
}

process.on('SIGINT',  () => { ws?.close(); process.exit(0); });
process.on('SIGTERM', () => { ws?.close(); process.exit(0); });

if (!TELEGRAM_BOT_TOKEN) {
  console.error('\n!!! TELEGRAM_BOT_TOKEN is not set - you will NOT get notifications.');
  console.error('!!! Start with:  TELEGRAM_BOT_TOKEN=your_token node indicators.js\n');
}

// ─── Health-check HTTP server ────────────────────────────────────────────────
// Hosting platforms require the app to listen on the PORT env var. This tiny
// server only answers health checks; it does not touch the bot logic above.
const http = require('http');
const PORT = process.env.PORT || 8080;

http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Deriv EMA bot is running\n');
}).listen(PORT, '0.0.0.0', () => {
  console.log(`[HTTP] Health-check server listening on port ${PORT}`);
});

initializeWebSocket();
