import { Consumer, ConsumerConfig, Kafka } from 'kafkajs';
import { Readable } from 'stream';

export class ConsumerObjectStream extends Readable {
  constructor(
    kafka: Kafka,
    options: {
      config?: ConsumerConfig;
      topics: { topic: string | RegExp; fromBeginning?: boolean }[];
    },
  ) {
    super({ objectMode: true });
    this.kafka = kafka;
    this.config = options.config;
    this.topics = options.topics;
    this.init();
  }

  private consumer: Consumer;

  private kafka: Kafka;

  private config?: ConsumerConfig;

  private topics: { topic: string | RegExp; fromBeginning?: boolean }[];

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
      this.consumer = this.kafka.consumer(this.config);
      await this.consumer.connect();
      for (const topic of this.topics) {
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
        for (const message of batch.messages) {
          if (this.paused) {
            break;
          }
          const continueToPush = this.push(message);
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
