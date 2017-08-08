package se.david.kafka.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import se.david.kafka.consumer.Receiver;
import se.david.kafka.producer.Sender;

import javax.validation.constraints.NotNull;
import javax.websocket.server.PathParam;

@RestController
public class ApiController {
    @Autowired
    private Sender sender;
    @Autowired
    private Receiver receiver;

    @GetMapping(path = "/kafka/{message}")
    ResponseEntity<String> triggerKafka(@PathVariable("message") @NotNull String message) {
        sender.send("receiver.t", message);

        return ResponseEntity.ok(receiver.getReceivedMessage());
    }
}
