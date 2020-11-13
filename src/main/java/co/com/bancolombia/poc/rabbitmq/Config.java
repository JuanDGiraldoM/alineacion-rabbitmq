package co.com.bancolombia.poc.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

@Configuration
public class Config {
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);

    @SneakyThrows
    @Bean
    public Mono<Connection> connectionMono(RabbitProperties rabbitProperties) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(rabbitProperties.getHost());
        connectionFactory.setPort(rabbitProperties.getPort());
        connectionFactory.setVirtualHost(rabbitProperties.getVirtualHost());
        if (rabbitProperties.getSsl().getEnabled()) {
            connectionFactory.useSslProtocol();
        }
        connectionFactory.setUsername(rabbitProperties.getUsername());
        connectionFactory.setPassword(rabbitProperties.getPassword());
        return Mono.fromCallable(() -> connectionFactory.newConnection("reactor-rabbit-sample")).cache();
    }

    @Bean
    public Sender sender(Mono<Connection> connectionMono) {
        return RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));
    }

    @Bean
    public Receiver receiver(Mono<Connection> connectionMono) {
        return RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));
    }

    @Bean
    public String queue(Sender sender, @Value("${app.user.queue-name}") String queueName) {
        // block for testing purpose
        sender.declareExchange(ExchangeSpecification.exchange("animal-direct").type("fanout")).block();
        sender.declare(QueueSpecification.queue(queueName)).block();
        sender.bindQueue(BindingSpecification.binding().queue(queueName).exchange("directMessages").routingKey(queueName))
                .block();
        LOGGER.debug("queue {} configured", queueName);
        return queueName;
    }

}
