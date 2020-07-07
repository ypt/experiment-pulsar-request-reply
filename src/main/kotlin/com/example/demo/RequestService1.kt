package com.example.demo

import java.nio.charset.StandardCharsets
import kotlin.random.Random
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.RegexSubscriptionMode
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionMode
import org.apache.pulsar.client.api.SubscriptionType

// The approach in this file creates a subscription per request
class RequestService1(
    private val serviceInstanceId: Int, // Should be unique
    private val messageBusClient: MessageBusClient
) {
    fun request(topic: String, message: String): String {
        try {
            val requestId = Random.nextInt()

            // It seems pretty wasteful to create a Consumer for each request.
            // Ideally, we can use a single subcription for incoming replies and demux them to the appropriate handlers
            val replyConsumer: Consumer<ByteArray> = messageBusClient.client.newConsumer()
                .topic("non-persistent://public/default/requestService.inbox.$serviceInstanceId.$requestId")
                .subscriptionTopicsMode(RegexSubscriptionMode.NonPersistentOnly)
                .subscriptionName("requestService.inbox.$serviceInstanceId.$requestId")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionMode(SubscriptionMode.NonDurable)
                .subscribe()

            val producer = messageBusClient.client.newProducer<String>(Schema.STRING)
                .topic(topic)
                .create()

            // I'm not sure if appending a unique requestId adds value, partitioning by a unique serviceInstanceId might be sufficient?
            // Alternatively, we can consider sending requestId with the request and returning it in the reply in a message property
            val replyTo = "non-persistent://public/default/requestService.inbox.$serviceInstanceId.$requestId"

            println("SENDING")
            println(message)
            println(replyTo)

            producer.newMessage()
                .value(message)
                .property("reply_to", replyTo)
                .property("request_id", requestId.toString())
                .sendAsync()
            val msg = replyConsumer.receive()

            println("RECEIVED")
            val reply = String(msg.data, StandardCharsets.UTF_8)
            println(reply)

            return reply
        } catch (e: Exception) {
            println(e)
            return ""
        }
    }
}
