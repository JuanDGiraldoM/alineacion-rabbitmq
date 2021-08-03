package co.com.bancolombia.poc.rabbitmq;

import com.rabbitmq.client.Delivery;
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
        LOGGER.info("Rest created");
        this.sender = sender;
    }

    @PostMapping("/send")
    private Mono<String> sendMessage(@RequestBody Message message) {
        LOGGER.info("Sending Message {}", message.getMessage());
        return sender.send(createMessage(message))
                .thenReturn("OK");
    }

    @PostMapping("/sendto")
    private Mono<String> requestReply(@RequestBody Message message) {
        LOGGER.info("Sending Message {}", message.getMessage());

        // TODO: 3. Create request for reply
        // Correlation ID
        Supplier<String> correlationId = () -> UUID.randomUUID().toString();
        // Create RPC Client with correlation ID param
        RpcClient client = sender.rpcClient(message.getExchange(), message.getRoutingKey(), correlationId);
        // Send body through rpc request
        RpcClient.RpcRequest request = new RpcClient.RpcRequest(message.getMessage().getBytes());
        // Get the response
        Mono<Delivery> response = client.rpc(Mono.just(request));
        // Return the delivery response
        return response.map(delivery -> new String(delivery.getBody()));
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
