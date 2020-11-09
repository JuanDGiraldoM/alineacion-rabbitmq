package co.com.bancolombia.poc.rabbitmq;

import com.rabbitmq.client.AMQP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.RpcClient;
import reactor.rabbitmq.Sender;

import java.util.UUID;
import java.util.function.Supplier;

@RestController
@RequestMapping("/rabbitmq")
public class SampleSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleSender.class);
    private final Sender sender;

    public SampleSender(Sender sender) {
        LOGGER.debug("Rest created");
        this.sender = sender;
    }

    @PostMapping("/send")
    private Mono<String> send(@RequestBody Message message) {
        LOGGER.debug("Sending Message {}", message.getMessage());
        return sender.send(createMessage(message))
                .thenReturn("OK");
    }

    @PostMapping("/req/reply")
    private Mono<String> reqReply(@RequestBody Message message) {
        LOGGER.debug("Sending Message {}", message.getMessage());
        Supplier<String> correlationIdSupplier = () -> UUID.randomUUID().toString();
        RpcClient client = sender.rpcClient(message.getExchange(), message.getRoutingKey(), correlationIdSupplier);
        Mono<RpcClient.RpcRequest> request = Mono.just(new RpcClient.RpcRequest("request".getBytes()));
        return client.rpc(request)
                .map(delivery -> new String(delivery.getBody()));
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
