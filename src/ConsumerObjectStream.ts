/* eslint-disable no-underscore-dangle, no-await-in-loop */
import { Consumer, ConsumerConfig, Kafka, KafkaMessage } from 'kafkajs';
import { Readable } from 'stream';

interface ConsumerObjectStreamOptions {
  config?: ConsumerConfig;
  topics: { topic: string | RegExp; fromBeginning?: boolean }[];
  highWaterMark?: number;
  transform?: (data: KafkaMessage) => any;
}

export class ConsumerObjectStream extends Readable {
  constructor(kafka: Kafka, options: ConsumerObjectStreamOptions) {
    super({ objectMode: true, highWaterMark: options.highWaterMark ?? 512 });
    this.kafka = kafka;
    this.options = options;
    this.init();
  }

  private consumer: Consumer;

  private kafka: Kafka;

  private options: ConsumerObjectStreamOptions;

  private connected: boolean;

  private started: boolean;

  private paused: boolean;

  private init() {
    this.connected = false;
    this.started = false;
    this.paused = false;
  }

  _read() {
    (async () => {
      try {
        await this.start();
      } catch (e) {
        this.destroy(e);
      }
    })();
  }

  private async start() {
    if (!this.connected) {
      this.connected = true;
      this.consumer = this.kafka.consumer(this.options.config);
      await this.consumer.connect();
      // eslint-disable-next-line no-restricted-syntax
      for (const topic of this.options.topics) {
        await this.consumer.subscribe(topic);
      }
      this.consumer.on('consumer.crash', this.onCrash);
    }
    if (!this.started) {
      this.started = true;
      await this.run();
    }
    if (this.paused) {
      this.paused = false;
    }
  }

  private onCrash = async (err: Error) => {
    console.error(err);
    this.init();
    await this.start();
  };

  private async run() {
    await this.consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        if (this.paused) {
          return;
        }
        // eslint-disable-next-line no-restricted-syntax
        for (const message of batch.messages) {
          if (this.paused) {
            break;
          }
          const payload = this.options.transform?.(message) ?? message;
          const continueToPush = this.push(payload);
          resolveOffset(message.offset);
          await heartbeat();
          if (!continueToPush) {
            this.paused = true;
          }
        }
      },
    });
  }

  _destroy(error: Error | null) {
    this.consumer.disconnect();
    super.destroy(error === null ? undefined : error);
  }
}
