package co.com.bancolombia.poc.rabbitmq;

import com.rabbitmq.client.AMQP;
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
    private final Sender sender;
    private final String queueName;

    public SampleReceiver(Sender sender, Receiver receiver, @Value("${app.user.queue-name}") String queueName) {
        this.receiver = receiver;
        this.sender = sender;
        this.queueName = queueName;
        listen();
    }

    public void listen() {
        Flux<Delivery> deliveryFlux = receiver.consumeAutoAck(queueName);

        deliveryFlux
                .map(this::process)
                .map(this::respond)
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

    private Delivery respond(Delivery delivery) {
        if ("request".equals(new String(delivery.getBody()))) {
            LOGGER.info("Replying to request");
            sender.send(createMessage(delivery, "reply"))
                    .subscribe();
        }
        return delivery;
    }


    private Mono<OutboundMessage> createMessage(Delivery delivery, String response) {
        String correlationId = delivery.getProperties().getCorrelationId();
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId).build();
        OutboundMessage outboundMessage = new OutboundMessage(
                "",
                delivery.getProperties().getReplyTo(),
                properties,
                response.getBytes()
        );
        return Mono.just(outboundMessage);
    }

}
