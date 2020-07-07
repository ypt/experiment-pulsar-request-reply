package com.example.demo

import kotlin.random.Random

fun main(args: Array<String>) {
    if (!(args.contains("--reply_service") || args.contains("--request_service"))) {
        println("ERROR: At least one of --reply_service --request_service must be specified")
        return
    }

    val messageBusClient = MessageBusClient(
        MessageBusClient.Config(
            serviceUrl = System.getenv("PULSAR_URL") ?: "pulsar://localhost:6650"
        )
    )

    if (args.contains("--reply_service")) {
        ReplyService(messageBusClient = messageBusClient)
    }

    if (args.contains("--request_service")) {
        val requestService = RequestService3(
            serviceInstanceId = Random.nextInt(),
            messageBusClient = messageBusClient
        )
        var line = readLine()
        while (line != null) {
            println("REQUEST: $line")
            val reply = requestService.request("persistent://public/default/replyService.command.shout", line)
            println("REPLY: $reply")
            line = readLine()
        }
    }
}
