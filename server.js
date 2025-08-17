// --- Dependencies ---
const express = require("express");
const WebSocket = require("ws");
const fetch = require("node-fetch");
const fs = require("fs");
const moment = require("moment-timezone");
const path = require("path");

// --- Config ---
const PORT = process.env.PORT || 5000;
const TZ = "Asia/Kolkata";
const CACHE_FILE = path.join(__dirname, "avgVolumeCache.json");
const MAX_BACKUPS = 3;
const DAYS = 3; // 3-day average
const TOP_N = 30;
const TRIGGER_HISTORY = 3;
const CACHE_TTL_HOURS = 6;

// --- State ---
let avgVolumes = {};
let lastTriggers = {};
let symbols = [];
const wsMap = {}; // symbol -> { ws, avg }

// --- Cache Helpers ---
function loadCache() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const cache = JSON.parse(fs.readFileSync(CACHE_FILE, "utf-8"));
      avgVolumes = {};
      const now = Date.now();
      for (const [symbol, entry] of Object.entries(cache.avgVolumes || {})) {
        const ts = new Date(entry.ts).getTime();
        if (now - ts < CACHE_TTL_HOURS * 3600 * 1000) {
          avgVolumes[symbol] = entry;
          console.log(`[CACHE HIT] ${symbol} avg restored from cache`);
        }
      }
    }
  } catch (err) {
    console.error("[CACHE] Load failed", err);
  }
}

function saveCache() {
  try {
    for (let i = MAX_BACKUPS - 1; i >= 1; i--) {
      const src = `${CACHE_FILE}.${i}`;
      const dst = `${CACHE_FILE}.${i + 1}`;
      if (fs.existsSync(src)) fs.renameSync(src, dst);
    }
    if (fs.existsSync(CACHE_FILE)) fs.renameSync(CACHE_FILE, `${CACHE_FILE}.1`);
    fs.writeFileSync(CACHE_FILE, JSON.stringify({ avgVolumes }, null, 2));
    console.log("[CACHE] Saved avg volumes");
  } catch (err) {
    console.error("[CACHE] Save failed", err);
  }
}

// --- Binance Helpers ---
async function fetch15mBaseline(symbol, days = DAYS) {
  try {
    const limit = days * 96;
    const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=15m&limit=${limit}`;
    const res = await fetch(url);
    const data = await res.json();
    if (Array.isArray(data) && data.length > 0) {
      const volumes = data.map(k => parseFloat(k[5]));
      const avg = volumes.reduce((a, b) => a + b, 0) / volumes.length;
      console.log(`[BASELINE] ${symbol} true 15m avg = ${avg.toFixed(2)}`);
      return avg;
    }
    throw new Error("15m klines invalid");
  } catch (err) {
    console.warn(`[FALLBACK-1] ${symbol} 15m fetch failed:`, err.message);
  }

  try {
    const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=1d&limit=${days}`;
    const res = await fetch(url);
    const data = await res.json();
    if (Array.isArray(data) && data.length > 0) {
      const dailyVolumes = data.map(k => parseFloat(k[5]));
      const avgDaily = dailyVolumes.reduce((a, b) => a + b, 0) / dailyVolumes.length;
      const avg15m = avgDaily / 96;
      console.log(`[BASELINE] ${symbol} daily ÷ 96 avg = ${avg15m.toFixed(2)}`);
      return avg15m;
    }
    throw new Error("1d klines invalid");
  } catch (err) {
    console.warn(`[FALLBACK-2] ${symbol} daily ÷ 96 failed:`, err.message);
  }

  try {
    const url = `https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=${symbol}`;
    const res = await fetch(url);
    const data = await res.json();
    if (data && data.volume) {
      const vol24h = parseFloat(data.volume);
      const avg15m = vol24h / 96;
      console.log(`[BASELINE] ${symbol} 24hr ÷ 96 avg = ${avg15m.toFixed(2)}`);
      return avg15m;
    }
    throw new Error("24hr ticker invalid");
  } catch (err) {
    console.error(`[ERROR] All baselines failed for ${symbol}`, err.message);
    return null;
  }
}

async function fetchTopPerpetualSymbols(n = TOP_N) {
  const url = "https://fapi.binance.com/fapi/v1/ticker/24hr";
  const res = await fetch(url);
  const data = await res.json();
  if (!Array.isArray(data)) throw new Error("Failed to fetch 24hr stats");

  const filtered = data.filter(d => d.symbol.endsWith("USDT"));
  filtered.sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume));
  return filtered.slice(0, n).map(d => d.symbol);
}

// --- WebSocket Handler ---
function setupSymbolWS(symbol, avg) {
  const ws = new WebSocket(`wss://fstream.binance.com/ws/${symbol.toLowerCase()}@kline_15m`);
  wsMap[symbol] = { ws, avg };

  ws.on("open", () => console.log(`[WS OPEN] ${symbol}`));

  ws.on("message", (msg) => {
    try {
      const data = JSON.parse(msg);
      if (!data.k || !data.k.x) return;

      const vol = parseFloat(data.k.v);
      const closePrice = parseFloat(data.k.c);
      const closeTime = data.k.T;
      const currentAvg = wsMap[symbol].avg;

      const trigger = vol >= currentAvg * 3;

      if (trigger) {
        const istTime = moment(closeTime).tz(TZ).format("YYYY-MM-DD h:mm A");
        if (!lastTriggers[symbol]) lastTriggers[symbol] = [];
        lastTriggers[symbol].unshift({
          time: istTime,
          volume: vol,
          avg: currentAvg,
          relVol: (vol / currentAvg).toFixed(2),
          ltp_inr: closePrice * 83
        });
        lastTriggers[symbol] = lastTriggers[symbol].slice(0, TRIGGER_HISTORY);
        console.log(`[TRIGGER] ${symbol} at ${istTime}, vol=${vol}, avg=${currentAvg.toFixed(2)}`);
        broadcast(lastTriggers);
      }
    } catch (err) {
      console.error("[WS ERROR]", err);
    }
  });

  ws.on("close", () => {
    console.log(`[WS CLOSED] ${symbol}, retrying...`);
    setTimeout(() => setupSymbolWS(symbol, wsMap[symbol].avg), 5000);
  });
}

// --- Update WS baseline ---
function updateBaseline(symbol, newAvg) {
  if (wsMap[symbol]) {
    wsMap[symbol].avg = newAvg;
    console.log(`[WS BASELINE UPDATED] ${symbol} = ${newAvg.toFixed(2)}`);
  }
}

// --- Refresh Baselines ---
async function refreshBaselines(targetSymbols = symbols) {
  console.log(`[REFRESH] Updating baselines @ ${moment().tz(TZ).format()}`);
  for (const symbol of targetSymbols) {
    const avg = await fetch15mBaseline(symbol, DAYS);
    if (avg) {
      avgVolumes[symbol] = { avg, ts: new Date().toISOString(), source: "3d-15m" };
      updateBaseline(symbol, avg);
    }
  }
  saveCache();
  console.log("[REFRESH] Baseline update complete");
}

// --- Schedule Daily Refresh @ 00:05 IST ---
function scheduleDailyRefresh() {
  function scheduleNext() {
    const now = moment.tz(TZ);
    const next = now.clone().add(1, "day").startOf("day").add(5, "minutes");
    const ms = next.diff(now);
    console.log(`[REFRESH] Next baseline update scheduled at ${next.format()}`);
    setTimeout(async () => {
      await refreshBaselines();
      scheduleNext();
    }, ms);
  }
  scheduleNext();
}

// --- WebSocket Broadcast ---
function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}

// --- Init ---
async function init() {
  loadCache();

  try {
    symbols = await fetchTopPerpetualSymbols(TOP_N);
    console.log("[TOP SYMBOLS]", symbols);
  } catch (err) {
    console.error("[ERROR] Could not fetch top symbols", err);
    return;
  }

  for (const symbol of symbols) {
    const cachedAvg = avgVolumes[symbol]?.avg;
    if (cachedAvg) {
      console.log(`[INIT] Using cached avg for ${symbol}: ${cachedAvg.toFixed(2)}`);
      setupSymbolWS(symbol, cachedAvg);
    } else {
      const avg = await fetch15mBaseline(symbol, DAYS);
      if (avg) {
        avgVolumes[symbol] = { avg, ts: new Date().toISOString(), source: "3d-15m" };
        saveCache();
        setupSymbolWS(symbol, avg);
      }
    }
  }

  scheduleDailyRefresh();
}

init();

// --- Express + WebSocket Server ---
const app = express();
app.use(express.static("public"));

app.get("/triggers", (req, res) => {
  res.json(lastTriggers);
});

// --- Manual Refresh Endpoint ---
app.get("/refresh-baselines", async (req, res) => {
  try {
    const targetSymbols = req.query.symbol
      ? [req.query.symbol.toUpperCase()]
      : symbols;
    await refreshBaselines(targetSymbols);
    res.json({ status: "ok", message: "Baselines refreshed manually", symbols: targetSymbols });
  } catch (err) {
    console.error("[ERROR] manual refresh", err);
    res.status(500).json({ status: "error", message: err.message });
  }
});

const server = app.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));
const wss = new WebSocket.Server({ server });



// //test
// const express = require("express");
// const WebSocket = require("ws");
// const app = express();
// const PORT = 5000;

// app.use(express.static("public")); // serve your index.html

// const server = app.listen(PORT, () => {
//   console.log(`Test server running at http://localhost:${PORT}`);
// });

// const wss = new WebSocket.Server({ server });

// // Function to generate random test data
// function generateTestData() {
//   const symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"];
//   const data = {};
//   symbols.forEach(symbol => {
//     const avg = Math.random() * 100 + 50;
//     const volume = avg * (Math.random() * 3 + 0.5); // 0.5x – 3.5x avg
//     const ltp_inr = Math.random() * 200000 + 50000;
//     const time = new Date().toLocaleTimeString("en-IN", { hour12: true });
//     data[symbol] = [{
//       time, volume, avg, ltp_inr
//     }];
//   });
//   return data;
// }

// // Broadcast test data every 5 seconds
// setInterval(() => {
//   const data = generateTestData();
//   wss.clients.forEach(client => {
//     if (client.readyState === WebSocket.OPEN) {
//       client.send(JSON.stringify(data));
//     }
//   });
// }, 5000);
