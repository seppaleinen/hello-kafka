package se.david.kafka.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import se.david.kafka.consumer.Receiver;
import se.david.kafka.producer.Sender;

import javax.validation.constraints.NotNull;

@RestController
public class ApiController {
    @Autowired
    private Sender sender;
    @Autowired
    private Receiver receiver;

    @GetMapping(path = "/kafka/{message}",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    ResponseEntity<String> triggerKafka(@PathVariable("message") @NotNull String message) {
        sender.send("receiver.t", message);

        return ResponseEntity.ok(receiver.getReceivedMessage());
    }
}
