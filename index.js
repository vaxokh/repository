const { Sender } = require('@questdb/nodejs-client');

const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));

const sender = Sender.fromConfig('http::addr=localhost:9000');

async function fetchAndStorePrices() {
  
  try {
  
    const response = await fetch('https://fapi.binance.com/fapi/v1/premiumIndex');
    const data = await response.json();

    const time = Date.now();

    for (const token of data) {
      const { symbol, markPrice } = token;
      if (!markPrice) continue;
      if (!symbol.endsWith("USDT"))continue;

      const price = parseFloat(markPrice);

      await sender.table('token_prices')
        .symbol("symbol",symbol.substring(-4, (symbol.length - 4)))
        .floatColumn("price",price)
        .at(time, "ms");
    }
    await sender.flush();
  } catch (error) {
    console.error('⚠ შეცდომა მონაცემის მიღების ან ჩაწერისას:', error);
  }
}

async function start() {
  try {
    
    console.log('📡 კავშირი წარმატებულია');

    await fetchAndStorePrices();

    setInterval(fetchAndStorePrices, 60 * 1000);

  } catch (err) {
    console.error('❌ ვერ მოხერხდა ბაზასთან კავშირი:', err);
  }
}


start();
 