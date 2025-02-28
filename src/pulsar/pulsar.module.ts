import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Client } from 'pulsar-client';
import { PULSAR_CLIENT } from './constants';
import { PulsarProducerService } from './pulsar-producer.service';

/**
 * Module for configuring and providing Pulsar integration.
 * This module sets up the Pulsar client and producer service, making them available for dependency injection within the application.
 * It utilizes the `ConfigModule` to retrieve Pulsar service URL from environment configuration.
 * @exports {PulsarModule} Exports the `PulsarModule` class, making it available for import in other modules.
 */
@Module({
  imports: [ConfigModule],
  providers: [
    PulsarProducerService,
    {
      inject: [ConfigService],
      provide: PULSAR_CLIENT,
      /**
       * Factory function that creates and provides the Pulsar Client instance.
       * This function is responsible for instantiating the Pulsar client using configuration from the ConfigService.
       * It retrieves the Pulsar service URL from the configuration and uses it to create a new Pulsar Client.
       * @param {ConfigService} configService - Injected ConfigService instance, used to access application configuration.
       * @returns {Client} A new Pulsar Client instance configured with the service URL from the application configuration.
       */
      useFactory: (configService: ConfigService) =>
        new Client({
          serviceUrl: configService.getOrThrow('PULSAR_SERVICE_URL'),
        }),
    },
  ],
  /**
   * Exports the PulsarProducerService and PULSAR_CLIENT providers.
   * This makes the Pulsar producer service and the Pulsar client instance available for injection in other modules that import PulsarModule.
   */
  exports: [PulsarProducerService, PULSAR_CLIENT],
})
export class PulsarModule {}
