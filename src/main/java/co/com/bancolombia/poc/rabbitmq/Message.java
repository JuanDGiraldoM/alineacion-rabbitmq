package co.com.bancolombia.poc.rabbitmq;

import lombok.Data;

@Data
public class Message {
    private String message;
    private String exchange;
    private String routingKey;
}
