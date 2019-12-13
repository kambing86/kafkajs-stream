import { Consumer, ConsumerConfig, Kafka } from "kafkajs";
import { Readable } from "stream";

class TooMuchError extends Error {
  message: "Too much data";
}

export class ConsumerStream extends Readable {
  constructor(
    kafka: Kafka,
    options: {
      config?: ConsumerConfig;
      topic: { topic: string; fromBeginning?: boolean };
    }
  ) {
    super();
    this.consumer = kafka.consumer(options.config);
    this.topic = options.topic;
    this.connected = false;
    this.started = false;
    this.paused = false;
  }

  private consumer: Consumer;

  private topic: { topic: string; fromBeginning?: boolean };

  private connected: boolean;

  private started: boolean;

  private paused: boolean;

  _read() {
    (async () => {
      try {
        if (!this.connected) {
          this.connected = true;
          await this.consumer.connect();
          await this.consumer.subscribe(this.topic);
        }
        if (!this.started) {
          this.started = true;
          await this.run();
        }
        if (this.paused) {
          this.paused = false;
          try {
            this.consumer.resume([{ topic: this.topic.topic }]);
          } catch (e) {
            // consumer might be stopped for some reasons, and calling resume will throw error
            await this.run();
          }
        }
      } catch (e) {
        this.destroy(e);
      }
    })();
  }

  private async run() {
    await this.consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
        for (const message of batch.messages) {
          const continueToPush = this.push(message.value);
          resolveOffset(message.offset);
          await heartbeat();
          if (!continueToPush) {
            throw new TooMuchError();
          }
        }
      }
    });
  }

  _destroy(error: Error | null) {
    this.consumer.disconnect();
    super.destroy(error === null ? undefined : error);
  }
}
