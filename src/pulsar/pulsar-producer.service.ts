import { Inject, Injectable } from '@nestjs/common';
import { Client, Producer } from 'pulsar-client';
import { PULSAR_CLIENT } from './constants';

/**
 * Injectable service responsible for producing messages to Pulsar topics.
 * Manages a collection of producers to efficiently send messages to different topics.
 */
@Injectable()
export class PulsarProducerService {
  /**
   * A map to store Pulsar producers, keyed by topic name.
   * This allows reusing producers for the same topic, improving efficiency.
   * @private
   * @readonly
   */
  private readonly producers = new Map<string, Producer>();

  /**
   * Constructor for PulsarProducerService.
   * @param  pulsarClient - Injected Pulsar client instance.
   * The Pulsar client is used to create and manage producers.
   * @Inject(PULSAR_CLIENT) Inject decorator to obtain the Pulsar Client from dependency injection.
   */
  constructor(@Inject(PULSAR_CLIENT) private readonly pulsarClient: Client) {}

  /**
   * Produces a `message` to the specified Pulsar `topic`.
   * If a `producer` for the `topic` already exists, it reuses it; otherwise, it creates a new one.
   * @async
   * @param {string} topic - The name of the Pulsar `topic` to which the `message` should be sent.
   * @param {any} message - The `message` payload to send. This will be serialized to JSON and sent as a `Buffer`.
   * @returns {Promise<void>} A Promise that resolves when the message has been successfully sent to the Pulsar topic.
   */
  async produce(topic: string, message: any): Promise<void> {
    let producer = this.producers.get(topic);
    if (!producer) {
      producer = await this.pulsarClient.createProducer({
        topic,
      });
      this.producers.set(topic, producer);
    }
    await producer.send({
      // Serialize message to JSON and convert to Buffer
      data: Buffer.from(JSON.stringify(message)),
    });
  }
}
