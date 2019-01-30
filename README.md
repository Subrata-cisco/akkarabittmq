# akkaAmqpTest

Backpressure is feature implemented by default for any kind of akka stream (nothing need to be done it is automatically provided).

Backpressure slows down the producer but does not block the producer until memory and disk runs out.

RabbitMQ does not provide any API to check how much slowdown it will cause as part of back pressure.

Several strategy can be implemented when Producer is fast and consumer is slow using OverFlowStrategy.
https://doc.akka.io/docs/akka/2.5.4/java/stream/stream-rate.html#buffers-in-akka-streams

There are few backpressure aware API , which can be used to achieve some other goals like agrregate message as single message etc.
https://doc.akka.io/docs/akka/current/stream/operators/index.html?language=scala#backpressure-aware-operators

Producers gets blocked when there is no memory available but consumers can consume it and memory will come down as consumer drain the message.

RabbitMq Connection factory and Alpakka ConnectionProvider class has addBlockedListerner/withExceptionHandler(handleBlockedListenerException) 
callback method to get Producer is blocked notification.
https://pubs.vmware.com/vfabricRabbitMQ32/index.jsp?topic=/com.vmware.vfabric.rabbitmq.3.2/rabbit-web-docs/connection-blocked.html 

RabbitMq Producers accept the messages to be sent always when server is up and does not tell upfront Success or Failure. It fails later on
with TimeOutException or other exception.(If it tells then we can retry this message later, JIRA issue created on rabbitmq site)
(https://github.com/akka/alpakka/pull/1330)

RabbitMQ providet API to find the total memory and available memory status , a periodic background thread can monitor it if less than threshold
then Producing message and thus not occupying more memory and disk can be restricted.
( http://127.0.0.1:15672/api/nodes/)


