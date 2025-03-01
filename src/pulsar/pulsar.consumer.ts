import { nextTick } from 'process';
import { Logger } from '@nestjs/common';
import { Client, Consumer, ConsumerConfig, Message } from 'pulsar-client';

/**
 * Abstract base class for creating Pulsar consumers in NestJS applications.
 * Provides a template for consuming messages from a Pulsar topic, handling connection, message reception,
 * processing, and acknowledgement. Consumers extending this class need to implement the `onMessageFn`
 * to define the message processing logic.
 *
 * @typeparam T The type of the message payload after JSON parsing. This should match the expected structure of messages
 *            on the subscribed Pulsar topic.
 */
export abstract class PulsarConsumer<T> {
  /**
   * The Pulsar consumer instance used to receive messages.
   * @private
   * @type {Consumer}
   */
  private consumer: Consumer;
  /**
   * Logger instance for PulsarConsumer, used for logging consumer activities and errors.
   * Initialized in the constructor with the consumer's topic name or a default name.
   * @private
   * @readonly
   * @type {Logger}
   */
  private readonly logger: Logger;
  /**
   * A flag to control the consumption loop. Set to `false` to stop the consumer from receiving more messages.
   * @protected
   * @type {boolean}
   */
  protected isRunning = true;

  /**
   * Constructor for PulsarConsumer class.
   * Initializes the consumer with a Pulsar client, consumer configuration, and a message processing function.
   * Also initializes the logger with the topic name for context-specific logging.
   *
   * @param {Client} pulsarClient The Pulsar client instance, injected via dependency injection, used to create consumers.
   * @param {ConsumerConfig} config The configuration object for the Pulsar consumer, defining topic subscription, subscription name, etc.
   * @param {(message: T) => void} onMessageFn A callback function that will be invoked for each message received by the consumer.
   *                                          This function is where message processing logic should be implemented in extending classes.
   */
  constructor(
    private readonly pulsarClient: Client,
    private readonly config: ConsumerConfig,
    private readonly onMessageFn: (message: T) => void,
  ) {
    // Initialize logger *after* config is set in the constructor, using topic name if available, otherwise 'Consumer'
    this.logger = new Logger(this.config.topic || 'Consumer', {
      timestamp: true,
    });
  }

  /**
   * Establishes a connection to the Pulsar topic and starts consuming messages.
   * Subscribes to the topic using the provided configuration and then initiates the message consumption loop
   * in a non-blocking manner using `nextTick` to avoid blocking the event loop during initial setup.
   * @protected
   * @async
   * @returns {Promise<void>} A Promise that resolves when the consumer is connected and consumption loop is initiated.
   */
  protected async connect(): Promise<void> {
    // Subscribe to a topic and create a consumer on it
    this.consumer = await this.pulsarClient.subscribe(this.config);
    // Avoid blocking the event loop, start consuming messages in the next tick
    nextTick(this.consume.bind(this) as () => Promise<void>);
    await this.consume(); // Also call consume once directly to ensure it starts immediately in case nextTick is delayed significantly
  }

  /**
   * The core message consumption loop.
   * This private method continuously receives messages from the Pulsar topic as long as `isRunning` is true.
   * For each message received, it parses the JSON payload, logs the message and its ID, invokes the `onMessageFn`
   * for processing, and acknowledges the message to remove it from the topic backlog.
   * Error handling is included for both message consumption and acknowledgement to ensure robustness.
   * @private
   * @async
   * @returns {Promise<void>} A Promise that resolves when the consumption loop completes (which typically happens when `isRunning` is set to false).
   */
  private async consume(): Promise<void> {
    while (this.isRunning) {
      let message: Message | undefined = undefined;
      try {
        // Receive message - this is a non-blocking call that waits for the next message to arrive
        message = await this.consumer.receive();
        const messageBuffer = message.getData();
        const data = JSON.parse(messageBuffer.toString()) as T; // Parse message payload from Buffer to JSON and cast to type T
        this.logger.log(data, message.getMessageId().toString()); // Log the received message and its ID
        this.onMessageFn(data); // Invoke the message processing function implemented in extending classes
      } catch (err) {
        // Log any errors that occur during message reception or processing
        this.logger.error('Error consuming', err);
      }
      try {
        if (message) {
          // Acknowledge message to remove it from backlog and prevent redelivery
          await this.consumer.acknowledge(message);
        }
      } catch (error) {
        this.logger.error('Error during message acknowledgement', error);
      }
    }
  }
}
