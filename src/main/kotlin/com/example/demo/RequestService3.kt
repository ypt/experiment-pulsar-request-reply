package com.example.demo

import java.nio.charset.StandardCharsets
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.random.Random
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionMode
import org.apache.pulsar.client.api.SubscriptionType

// Here's another approach to leveraging a single subscriber that awaits for replies. Upon receiving a reply, it is
// routed to the appropriate handler.
//
// Instead of using unique topics _per request_ (which required regex subscription), use unique topics
// _per service instance_. This allows us to not use regex subscriptions, which didn't pick up newly created topics.
// With this approach, instead of using topic name to map a reply to a request, we need to have the request id
// in the reply message properties to do so.
class RequestService3(
    private val serviceInstanceId: Int, // Should be unique
    private val messageBusClient: MessageBusClient
) {
    private val replyTo = "non-persistent://public/default/requestService.inbox.$serviceInstanceId"
    private val replyConsumer: Consumer<ByteArray> = messageBusClient.client.newConsumer()
        .topic(replyTo)
        .subscriptionName("requestService.inbox.$serviceInstanceId")
        .subscriptionType(SubscriptionType.Exclusive)
        .subscriptionMode(SubscriptionMode.NonDurable)
        .subscribe()
    private val requestsInFlight: MutableMap<String, RequestInFlight> = mutableMapOf()
    private val requestsInFlightLock = ReentrantLock()
    private val producers: MutableMap<String, Producer<String>> = mutableMapOf()

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
        val requestId: String,
        val signalDone: Condition?,
        var reply: String?
    )

    class ConsumerRunnable(
        private val consumer: Consumer<ByteArray>,
        private val requestsInFlight: Map<String, RequestInFlight>,
        private val requestsInFlightLock: ReentrantLock
    ) : Runnable {
        override fun run() {
            while (true) {
                val msg = consumer.receive()
                try {
                    val message = String(msg.data, StandardCharsets.UTF_8)
                    val requestId = msg.properties["source_request_id"]

                    requestsInFlightLock.lock()
                    val requestInFlight = requestsInFlight[requestId]
                    if (requestInFlight != null) {
                        requestInFlight.reply = message
                        requestInFlight.signalDone?.signalAll()
                    }
                    requestsInFlightLock.unlock()

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

    // TODO: return a CompletableFuture?
    // fun requestAsync(topic: String, message: String) {
    // }

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

    fun request(topic: String, message: String): String {
        try {
            val producer = getProducer(topic)
            val requestId = Random.nextInt().toString()
            val replyReceivedCondition = requestsInFlightLock.newCondition()
            requestsInFlightLock.lock()
            val request = RequestInFlight(
                requestId = requestId,
                signalDone = replyReceivedCondition,
                reply = null
            )
            requestsInFlight[requestId] = request
            producer.newMessage()
                .value(message)
                .property("reply_to", replyTo)
                .property("request_id", requestId)
                .sendAsync()
            replyReceivedCondition.await() // TODO: await timeout
            requestsInFlight.remove(requestId)
            return request.reply!!
        } finally {
            requestsInFlightLock.unlock()
        }
    }
}
