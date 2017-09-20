package se.david.kafka.consumer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Receiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

    private String receivedMessage;

    @KafkaListener(topics = "${topic.receiver}")
    private void receive(String message) {
        LOGGER.info("received message='{}'", message);
        receivedMessage = message;
    }

    public String getReceivedMessage() {
        return receivedMessage;
    }
}
