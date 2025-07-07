const { Client } = require('pg');
const axios = require('axios');
const cron = require('node-cron');

// PostgreSQL client config for QuestDB
const client = new Client({
  user: 'admin',            // default user for QuestDB
  host: 'localhost',
  database: 'qdb',          // default QuestDB database name
  password: 'quest',        // default QuestDB password (can also be empty)
  port: 8812,               // QuestDB uses port 8812 (not 5432!)
});

async function start() {
  try {
    await client.connect();
    console.log('✅ Connected to QuestDB');

    // Run every minute
    cron.schedule('* * * * *', async () => {
      try {
        // Get BTC price from Binance
        const response = await axios.get('https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT');
        const price = parseFloat(response.data.price);
        const now = new Date().toISOString(); // format for QuestDB timestamp

        // Insert into QuestDB
        await client.query(
          'INSERT INTO bitcoin_prices(time, price) VALUES ($1, $2)',
          [now, price]
        );

        console.log(`✅ Inserted BTC price: $${price} at ${now}`);
      } catch (error) {
        console.error('❌ Error inserting:', error);
      }
    });

  } catch (err) {
    console.error('❌ Connection error:', err);
  }
}

start();
