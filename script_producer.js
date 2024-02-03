const express = require('express');
const { Kafka } = require('kafkajs');
const cors = require('cors');
const WebSocket = require('ws');
const http = require('http');


const app = express();
const port = 8000;

const fs = require('fs');
const path = require('path');

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
 
// Define Kafka broker configuration
const kafkaBrokerUrl = 'kafka-321cbf90-location.a.aivencloud.com:15944';  

const kafka = new Kafka({
  clientId: 'location-producer',
  brokers: [kafkaBrokerUrl],
  ssl: {
    ca : [fs.readFileSync(path.join(__dirname, 'ca.pem'), 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'avnadmin',
    password: 'AVNS_Y_5WSiNJAWxVC59hn6L',
  },
});

let producer = null;

async function connectToKafka() {
    try {
      if (producer) {
        return;
      }
      producer = kafka.producer();
      await producer.connect();
      console.log('Connected to Kafka');
    } catch (error) {
      console.error('Error connecting to Kafka:', error);
    }
  }

  const run = async (latitude , longitude) => {
    try {
        // await producer.connect() 
        await producer.send({
            topic: 'location',
            messages: [
                { value: JSON.stringify({ latitude , longitude }) },
            ],
        });
        console.log("done");
    } catch (error) {
        console.error('Error in run function:', error);
    }
}

wss.on('connection', (ws) => {
    ws.on('message', async (message) => {
        connectToKafka();
        const location = JSON.parse(message);
        const latitude = location.latitude;
        const longitude = location.longitude;
        await run(latitude , longitude).catch(console.error);
    });
  });

async function startServer() {
    await connectToKafka();
    server.listen(port, () => {
      console.log(`Server listening`);
    });
  }

startServer().catch(console.error());


app.get('/', (req, res) => {
  res.render('index_producer.ejs');
})