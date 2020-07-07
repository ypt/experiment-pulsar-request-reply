package com.example.demo

import java.nio.charset.StandardCharsets
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import java.util.regex.Pattern
import kotlin.random.Random
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.RegexSubscriptionMode
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionMode
import org.apache.pulsar.client.api.SubscriptionType

// The approach in this file leverages a single subscriber that awaits for replies. Upon receiving a reply, it is routed
// to the appropriate handler.
//
// Unfortunately, the particular implementation in this file does NOT work! Multi-topic subscriptions via regex do NOT
// "detect" new topics quick enough. See: https://github.com/apache/pulsar/issues/7157
//
// An example:
//
// docker run --rm -it \
// -p 6650:6650 \
// -p 8080:8080 \
// --name pulsar \
// apachepulsar/pulsar:2.6.0 \
// bin/pulsar standalone
//
// # In another window
// docker exec -it pulsar /bin/bash
// /pulsar/bin/pulsar-client consume non-persistent://public/default/.* --regex -s test
//
// # In another window
// docker exec -it pulsar /bin/bash
// /pulsar/bin/pulsar-client produce non-persistent://public/default/mytopic -m hello
//
// # Both non-persistent and persistent topics seem to be affected
// # In another window
// docker exec -it pulsar /bin/bash
// /pulsar/bin/pulsar-client consume persistent://public/default/.* --regex -s test
//
// # In another window
// docker exec -it pulsar /bin/bash
// /pulsar/bin/pulsar-client produce persistent://public/default/mytopic -m hello
//
// -----
//
// And a code demo of the issue:
//
// https://github.com/apache/pulsar/issues/7157
// val serviceUrl = System.getenv("PULSAR_URL") ?: "pulsar://localhost:6650"
// val pulsarClient = PulsarClient
//     .builder()
//     .serviceUrl(serviceUrl)
//     .build()
// pulsarClient.newConsumer()
//     .topicsPattern(Pattern.compile("non-persistent://public/default/sometopic.*"))
//     .subscriptionTopicsMode(RegexSubscriptionMode.NonPersistentOnly)
//     .subscriptionName("requestService.inbox.${Random.nextInt()}")
//     .subscriptionType(SubscriptionType.Exclusive)
//     .subscriptionMode(SubscriptionMode.NonDurable)
//     .messageListener { consumer, msg ->
//         println("RECEIVED MESSAGE")
//         println(msg.data.toString())
//         consumer.acknowledge(msg)
//     }
//     .subscribe()
// println("SENDING MESSAGE")
// pulsarClient.newProducer<String>(Schema.STRING)
//     .topic("non-persistent://public/default/sometopic.${Random.nextInt()}")
//     .create()
//     .newMessage()
//     .value("hello")
//     .sendAsync()

class RequestService2(
    private val serviceInstanceId: Int, // Should be unique
    private val messageBusClient: MessageBusClient
) {
    private val replyConsumer: Consumer<ByteArray> = messageBusClient.client.newConsumer()
        .topicsPattern(Pattern.compile("non-persistent://public/default/requestService\\.inbox\\.$serviceInstanceId\\.*"))
        .subscriptionTopicsMode(RegexSubscriptionMode.NonPersistentOnly)
        .subscriptionName("requestService.inbox.$serviceInstanceId")
        .subscriptionType(SubscriptionType.Exclusive)
        .subscriptionMode(SubscriptionMode.NonDurable)
        .subscribe()
    private val requestsInFlight: MutableSet<RequestInFlight> = emptySet<RequestInFlight>().toMutableSet()
    private val requestsInFlightLock = ReentrantLock()

    init {
        Thread(
            ConsumerRunnable(
                consumer = replyConsumer,
                requestsInFlight = requestsInFlight,
                requestsInFlightLock = requestsInFlightLock
            )
        ).start()
    }

    data class RequestInFlight(
        val requestId: Int,
        val signalDone: Condition?,
        var reply: String?
    )

    class ConsumerRunnable(
        private val consumer: Consumer<ByteArray>,
        private val requestsInFlight: MutableSet<RequestInFlight>,
        private val requestsInFlightLock: ReentrantLock
    ) : Runnable {
        override fun run() {
            println("STARTING CONSUMER")
            while (true) {
                val msg = consumer.receive()
                try {
                    val message = String(msg.data, StandardCharsets.UTF_8)
                    println("RECEIVING MESSAGE")
                    println(message)

                    val requestId = msg.topicName.split('.').last()
                    val requestInFlight = requestsInFlight.find { it.requestId.toString() == requestId }
                    if (requestInFlight != null) {
                        requestsInFlightLock.lock()
                        requestInFlight.reply = message
                        requestInFlight.signalDone?.signalAll()
                        requestsInFlightLock.unlock()
                    }

                    consumer.acknowledge(msg)
                } catch (e: Exception) {
                    consumer.negativeAcknowledge(msg)
                } finally {
                    if (requestsInFlightLock.isHeldByCurrentThread) {
                        requestsInFlightLock.unlock()
                    }
                }
            }
        }
    }

    // TODO: return a Promise?
    // fun requestAsync(topic: String, message: String) {
    // }

    fun request(topic: String, message: String): String {
        try {
            val producer = messageBusClient.client.newProducer<String>(Schema.STRING)
                .topic(topic)
                .create()

            val requestId = Random.nextInt()
            val replyReceivedCondition = requestsInFlightLock.newCondition()
            requestsInFlightLock.lock()
            val request = RequestInFlight(
                requestId = requestId,
                signalDone = replyReceivedCondition,
                reply = null
            )
            requestsInFlight.add(request)
            // TODO: not sure if appending a unique requestId adds value, partitioning by a unique serviceInstanceId might be sufficient?
            //  Alternatively, we can consider sending requestId with the request and returning it in the reply in a message property
            val replyTo = "non-persistent://public/default/requestService.inbox.$serviceInstanceId.$requestId"

            println("SENDING")
            println(request)
            println(message)
            println(replyTo)

            producer.newMessage()
                .value(message)
                .property("reply_to", replyTo)
                .property("request_id", requestId.toString())
                .sendAsync()
            replyReceivedCondition.await() // TODO: await timeout
            requestsInFlight.remove(request)
            return request.reply!!
        } finally {
            requestsInFlightLock.unlock()
        }
    }
}
