/* eslint-disable no-underscore-dangle */
import { CompressionTypes, Kafka, Producer, ProducerConfig } from "kafkajs";
import { Writable } from "stream";

export class ProducerStream extends Writable {
  constructor(
    kafka: Kafka,
    options: {
      config?: ProducerConfig;
      compression?: CompressionTypes;
      topic: string;
    },
  ) {
    super();
    this.producer = kafka.producer(options.config);
    this.compression = options.compression;
    this.connected = false;
    this.topic = options.topic;
  }

  private producer: Producer;

  private compression?: CompressionTypes;

  private topic: string;

  private connected: boolean;

  write(
    value: Uint8Array | Buffer | string,
    encoding?: string | ((error: Error | null | undefined) => void),
    callback?: (error: Error | null | undefined) => void,
  ): boolean {
    if (encoding === undefined) {
      return super.write(value);
    }
    if (typeof encoding === "string") {
      return super.write(value, encoding, callback);
    }
    return super.write(value, encoding);
  }

  _writev(
    chunks: { chunk: Uint8Array | Buffer | string; encoding: string }[],
    callback: (error?: Error | null) => void,
  ) {
    (async () => {
      try {
        if (!this.connected) {
          this.connected = true;
          await this.producer.connect();
        }
        await this.producer.send({
          topic: this.topic,
          compression: this.compression,
          // @ts-ignore Buffer.from with Uint8Array | Buffer | string has issue
          messages: chunks.map(({ chunk }) => ({ value: Buffer.from(chunk) })),
        });
        callback(null);
      } catch (e) {
        callback(e);
      }
    })();
  }

  _destroy(error: Error | null) {
    this.producer.disconnect();
    super.destroy(error === null ? undefined : error);
  }
}
