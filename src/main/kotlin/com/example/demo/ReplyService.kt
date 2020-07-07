package com.example.demo

import java.nio.charset.StandardCharsets
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType

class ReplyService(
    private val messageBusClient: MessageBusClient
) {
    private val producers: MutableMap<String, Producer<String>> = mutableMapOf()
    private val consumer: Consumer<ByteArray> = messageBusClient.client.newConsumer()
        .topic("persistent://public/default/replyService.command.shout")
        .subscriptionName("requestService")
        .subscriptionType(SubscriptionType.Shared)
        .messageListener { consumer, msg ->
            val message = String(msg.data, StandardCharsets.UTF_8)
            println("RECEIVED: $message")

            val replyTo = msg.properties["reply_to"]
            val requestId = msg.properties["request_id"]

            // Some work happens...
            val transformedMessage = message.toUpperCase() + "!!!!!!!"

            // Send a reply if one was requested
            if (replyTo != null && requestId != null) {
                println("SENDING REPLY TO ($replyTo) FOR REQUEST ($requestId)")
                val producer = getProducer(replyTo)
                producer.newMessage()
                    .value(transformedMessage)
                    .property("source_request_id", requestId)
                    .sendAsync()
            }
            consumer.acknowledge(msg)
        }
        .subscribe()

    private fun getProducer(topic: String): Producer<String> {
        val producer = producers[topic]
        return if (producer != null) {
            producer
        } else {
            val newProducer = messageBusClient.client.newProducer<String>(Schema.STRING)
                .topic(topic)
                .create()
            producers[topic] = newProducer
            return newProducer
        }
    }
}
