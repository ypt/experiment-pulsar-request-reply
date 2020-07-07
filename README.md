# experiment-pulsar-request-reply

A learning project experimenting with request-reply semantics on [Pulsar](https://pulsar.apache.org/en/). Inspired by [NATS](https://docs.nats.io/nats-concepts/reqreply).

```
     1 |     ^
       v   6 |
---------------------           ---------------------
|  request service  |           |   reply service   |
---------------------           ---------------------
     2 |      ^                         ^   4 |
       v    5 |                       3 |     v
--------------------------------------------------------
|                  Pulsar message bus                  |
--------------------------------------------------------
```

## Run it

Start Pulsar
```shell
docker run --rm -it \
  -p 6650:6650 \
  -p 8080:8080 \
  --name pulsar \
  apachepulsar/pulsar:2.6.0 \
  bin/pulsar standalone
```

Build the demo application
```shell
./gradlew build
```

Start the reply service
```shell
java -jar build/libs/demo-0.0.1-SNAPSHOT.jar --reply_service
```

Start the request service
```shell
java -jar build/libs/demo-0.0.1-SNAPSHOT.jar --request_service
```

Send some input to the request service on `stdin` and watch the replies come back.

For example
```
hello
REQUEST: hello
REPLY: HELLO!!!!!!!
```

Or pipe input into `stdin`
```
cat README.md | java -jar build/libs/demo-0.0.1-SNAPSHOT.jar --request_service
```

## Misc notes

You can docker exec into the Pulsar container and use [pulsar-client](https://pulsar.apache.org/docs/en/reference-cli-tools/#pulsar-client) 
to debug and observe what's happening on Pulsar.
```shell
/pulsar/bin/pulsar-client produce non-persistent://public/default/topic -m somemessage 
/pulsar/bin/pulsar-client consume non-persistent://public/default/topic -s subscribername   
```






