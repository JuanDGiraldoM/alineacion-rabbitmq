package co.com.bancolombia.poc.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.Sender;

@Component
@DependsOn({"queue"})
public class SampleReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleReceiver.class);
    private final Receiver receiver;
    private final String queueName;

    // TODO: 4. Implement Reply with sender
    private final Sender sender;

    public SampleReceiver(Receiver receiver, @Value("${app.user.queue-name}") String queueName, Sender sender) {
        this.receiver = receiver;
        this.queueName = queueName;
        // Add sender
        this.sender = sender;
        listen();
    }

    public void listen() {
        Flux<Delivery> deliveryFlux = receiver.consumeAutoAck(queueName);

        deliveryFlux
                .map(this::process)
                // Send message
                .flatMap(delivery -> sender.send(createMessage(delivery)))
                .subscribe();
    }

    private Delivery process(Delivery delivery) {
        LOGGER.info("Received message [exchange: {}, routingKey: {}, correlationId: {}] | {}",
                delivery.getEnvelope().getExchange(),
                delivery.getEnvelope().getRoutingKey(),
                delivery.getProperties().getCorrelationId(),
                new String(delivery.getBody())
        );
        return delivery;
    }

    // Implement reply message
    private Mono<OutboundMessage> createMessage(Delivery delivery) {
        BasicProperties properties = delivery.getProperties();
        AMQP.BasicProperties propertiesResponse = new AMQP.BasicProperties.Builder()
                .correlationId(properties.getCorrelationId())
                .build();
        OutboundMessage outboundMessage = new OutboundMessage(
                "",
                properties.getReplyTo(),
                propertiesResponse,
                "Hello World from jgmarin".getBytes()
        );
        return Mono.just(outboundMessage);
    }

}
