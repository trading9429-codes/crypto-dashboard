module.exports = {
  PORT: 7000,
  TZ: "Asia/Kolkata",

  // Binance settings
  BASE_URL: "https://fapi.binance.com",
  SYMBOL_LIMIT: 60,           // number of pairs to monitor
  REL_VOL_THRESHOLD: 1.0,     // RelVol > threshold → trigger

  // Optimization
  CANDLE_INTERVAL: "15m",     // timeframe
  AVG_DAYS: 20,               // 20-day average
  REFRESH_INTERVAL: 60 * 1000 // refresh 1m
};
