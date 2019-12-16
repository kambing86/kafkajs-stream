# Stream for Kafka.js in Node.js

tested in Node.js v12

## How to use

```sh
yarn add kafkajs-stream
```

```sh
npm install kafkajs-stream
```

## Example

```ts
import fs from 'fs';
import http from 'http';
import gracefulShutdown from 'http-graceful-shutdown';
import { CompressionCodecs, CompressionTypes, Kafka } from 'kafkajs';
import SnappyCodec from 'kafkajs-snappy';
import { ConsumerStream, ProducerStream } from 'kafkajs-stream';
import { range } from 'rxjs';
import { rxToStream } from 'rxjs-stream';
import { map } from 'rxjs/operators';
import app from './app';

const { PORT = 4000 } = process.env;

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['bitnami-kafka:9092'],
});

// Producing
const producerStream = new ProducerStream(kafka, { topic: 'test-topic' });
const number$ = range(0, 200000).pipe(map(i => `${i.toString()},`));
rxToStream(number$).pipe(producerStream);

// Consuming
const consumerStream = new ConsumerStream(kafka, {
  config: { groupId: 'test-group' },
  topic: { topic: 'test-topic', fromBeginning: true },
});
const producer2Stream = new ProducerStream(kafka, { topic: 'test-topic-2' });
consumerStream.pipe(producer2Stream);
consumerStream.on('error', err => {
  console.error('consumerStream', err);
});

const consumerStream2 = new ConsumerStream(kafka, {
  config: { groupId: 'test-group-2' },
  topic: { topic: 'test-topic-2', fromBeginning: true },
});
const writeStream = fs.createWriteStream('./testWrite.txt');
consumerStream2.pipe(writeStream);
consumerStream2.on('error', err => {
  console.error('consumerStream2', err);
});

(async () => {
  const server = http.createServer(app);
  server.listen(PORT);

  // register graceful shutdown
  gracefulShutdown(server, {
    onShutdown: async () => {
      await new Promise(resolve => producerStream.end(resolve));
      consumerStream.destroy();
      await new Promise(resolve => producer2Stream.end(resolve));
      consumerStream2.destroy();
      await new Promise(resolve => writeStream.end(resolve));
    },
  });

  server.on('listening', () => {
    console.log(`application is listening on port ${PORT}`);
  });
})();
```
