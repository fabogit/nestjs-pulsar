import { nextTick } from 'process';
import { Logger, OnModuleInit } from '@nestjs/common';
import { Client, Consumer, ConsumerConfig, Message } from 'pulsar-client';

/**
 * Abstract base class for creating Pulsar consumers in NestJS applications.
 * Provides a template for consuming messages from a Pulsar topic, handling connection, message reception,
 * processing, and acknowledgement. Consumers extending this class need to implement the {@link handleMessage}
 * method to define the specific message processing logic.
 *
 * @typeparam T The type of the message payload after JSON parsing. This should match the expected structure of messages
 *            on the subscribed Pulsar topic.
 * @implements {OnModuleInit} Implements the NestJS {@link OnModuleInit} interface to leverage module lifecycle hooks.
 */
export abstract class PulsarConsumer<T> implements OnModuleInit {
  /**
   * The Pulsar consumer instance used to receive messages.
   * Initialized in the {@link connect} method upon module initialization.
   * @private
   * @type {Consumer}
   */
  private consumer: Consumer;
  /**
   * Logger instance for PulsarConsumer, used for logging consumer activities, messages, and errors.
   * Initialized in the constructor with the consumer's topic name or a default name.
   * @protected
   * @readonly
   * @type {Logger}
   */
  protected readonly logger: Logger;
  /**
   * A flag to control the message consumption loop.
   * When set to `false`, the consumer will stop receiving new messages and the consumption loop will terminate.
   * Defaults to `true` and should be managed by extending classes if control over the consumption loop is needed.
   * @protected
   * @type {boolean}
   * @default true
   */
  protected isRunning: boolean = true;

  /**
   * Constructor for the abstract PulsarConsumer class.
   * Initializes the consumer with a Pulsar client, consumer configuration, and sets up the logger.
   * The message handling logic is expected to be implemented in the abstract {@link handleMessage} method by extending classes.
   *
   * @param {Client} pulsarClient The Pulsar client instance, injected via dependency injection, used to create consumers.
   * @param {ConsumerConfig} config The configuration object for the Pulsar consumer, defining topic subscription, subscription name, etc.
   */
  constructor(
    private readonly pulsarClient: Client,
    private readonly config: ConsumerConfig,
  ) {
    // Initialize logger *after* config is set in the constructor, using topic name if available, otherwise 'Consumer' as fallback
    this.logger = new Logger(this.config.topic || 'Consumer', {
      timestamp: true,
    });
  }

  /**
   * Establishes a connection to the Pulsar topic and starts the message consumption process.
   * Subscribes to the topic using the provided consumer configuration and initiates the {@link consume} loop
   * in a non-blocking manner using `nextTick` to prevent blocking the event loop during startup.
   * @protected
   * @async
   * @returns {Promise<void>} A Promise that resolves when the consumer is connected and the consumption loop is initiated.
   */
  protected async connect(): Promise<void> {
    // Subscribe to a topic and create a consumer on it
    this.consumer = await this.pulsarClient.subscribe(this.config);
    // Avoid blocking the event loop, start consuming messages in the next tick
    nextTick(this.consume.bind(this) as () => Promise<void>);
    await this.consume(); // Also call consume once directly to ensure it starts immediately in case nextTick is delayed significantly
  }

  /**
   * Lifecycle hook, called once the host module has been initialized.
   * It triggers the connection to the Pulsar topic and starts message consumption when the module initializes.
   * @async
   * @returns {Promise<void>} A Promise that resolves when the connection is established and consumption starts.
   * @implements {OnModuleInit}
   */
  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  /**
   * The core message consumption loop.
   * This private method continuously receives messages from the Pulsar topic as long as {@link isRunning} is true.
   * For each message received, it parses the JSON payload, logs the message and its ID, invokes the abstract {@link handleMessage}
   * method for custom processing, and acknowledges the message to remove it from the topic backlog.
   * Error handling is included for both message consumption and acknowledgement to ensure robust operation.
   * @private
   * @async
   * @returns {Promise<void>} A Promise that resolves when the consumption loop completes (typically when {@link isRunning} is set to false).
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
        this.handleMessage(data); // Invoke the message processing function implemented in extending classes
      } catch (err) {
        // Log any errors that occur during message reception or processing
        this.logger.error('Error consuming message', err);
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

  /**
   * Abstract method to be implemented by extending classes to define the message processing logic.
   * This method is invoked for each message received by the consumer after successful reception and parsing.
   * Extending classes must provide a concrete implementation of this method to handle the incoming messages of type `T`.
   * @protected
   * @abstract
   * @param {T} data The parsed message payload of type `T`.
   * @returns {void}
   */
  protected abstract handleMessage(data: T): void;
}
