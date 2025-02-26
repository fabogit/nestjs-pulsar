import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Client } from 'pulsar-client';
import { PULSAR_CLIENT } from './constants';

@Module({
  imports: [ConfigModule],
  providers: [
    {
      inject: [ConfigService],
      provide: PULSAR_CLIENT,
      useFactory: (configService: ConfigService) =>
        new Client({
          serviceUrl: configService.getOrThrow('PULSAR_SERVICE_URL'),
        }),
    },
  ],
})
export class PulsarModule {}
