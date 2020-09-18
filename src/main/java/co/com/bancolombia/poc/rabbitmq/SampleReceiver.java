package co.com.bancolombia.poc.rabbitmq;

import com.rabbitmq.client.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.Receiver;

@Component
@DependsOn({"queue"})
public class SampleReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleReceiver.class);
    private final Receiver receiver;
    private final String queueName;

    public SampleReceiver(Receiver receiver, @Value("${app.user.queue-name}") String queueName) {
        this.receiver = receiver;
        this.queueName = queueName;
        listen();
    }

    public void listen() {
        Flux<Delivery> deliveryFlux = receiver.consumeAutoAck(queueName);

        deliveryFlux
                .map(this::process)
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

}
