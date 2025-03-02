import { Inject, Injectable } from '@nestjs/common';
import { Client } from 'pulsar-client';
import { PulsarConsumer } from './pulsar/pulsar.consumer';
import { PULSAR_CLIENT } from './pulsar/constants';
import { TEST_TOPIC } from './app.constants';

/**
 * Injectable service that acts as a consumer for messages from a Pulsar topic.
 * Extends the abstract PulsarConsumer class to implement specific message handling logic for the application.
 * This consumer subscribes to the topic defined by `TEST_TOPIC` and processes messages using the `handleMessage` method.
 */
@Injectable()
export class AppConsumer extends PulsarConsumer<any> {
  /**
   * Constructor for AppConsumer.
   * Initializes the consumer by calling the super constructor of PulsarConsumer with the Pulsar client and consumer configuration.
   * The consumer is configured to subscribe to the topic defined by `TEST_TOPIC` with a subscription name 'nestjs'.
   * @param {Client} pulsarClient - Injected Pulsar client instance.
   * @Inject(PULSAR_CLIENT) - Inject decorator to obtain the Pulsar Client from dependency injection.
   */
  constructor(@Inject(PULSAR_CLIENT) pulsarClient: Client) {
    super(pulsarClient, { topic: TEST_TOPIC, subscription: 'nestjs' });
  }

  /**
   * Handles incoming messages from the Pulsar topic.
   * This method is invoked for each message received by the consumer.
   * It logs the received message data using the consumer's logger.
   * @param {any} data - The data payload of the message received from the Pulsar topic. The type is 'any' as per the generic type definition in `PulsarConsumer<any>`.
   * @returns {void}
   */
  handleMessage(data: any): void {
    this.logger.log('New message', data);
  }
}
