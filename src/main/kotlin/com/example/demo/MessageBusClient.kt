package com.example.demo

import org.apache.pulsar.client.api.PulsarClient

class MessageBusClient(config: Config) {
    data class Config(
        val serviceUrl: String
    )

    val client: PulsarClient = build(
        serviceUrl = config.serviceUrl
    )

    fun destroy() {
        client.close()
    }

    companion object {
        fun build(
            serviceUrl: String
        ): PulsarClient {
            var clientBuilder = PulsarClient
                .builder()
                .serviceUrl(serviceUrl)
            return clientBuilder.build()
        }
    }
}
