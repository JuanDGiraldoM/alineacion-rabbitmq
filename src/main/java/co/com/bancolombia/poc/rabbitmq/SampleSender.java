package co.com.bancolombia.poc.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.Sender;

@RestController
@RequestMapping("/rabbitmq")
public class SampleSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleSender.class);
    private final Sender sender;

    public SampleSender(Sender sender) {
        LOGGER.info("Rest created");
        this.sender = sender;
    }

    @PostMapping("/send")
    private Mono<String> sendMessage(@RequestBody Message message) {
        LOGGER.info("Sending Message {}", message.getMessage());
        return sender.send(createMessage(message))
                .thenReturn("OK");
    }

    private Mono<OutboundMessage> createMessage(Message message) {
        OutboundMessage outboundMessage = new OutboundMessage(
                message.getExchange(),
                message.getRoutingKey(),
                message.getMessage().getBytes()
        );
        return Mono.just(outboundMessage);
    }

}
