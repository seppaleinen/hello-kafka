package se.david.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import se.david.kafka.consumer.Receiver;
import se.david.kafka.producer.Sender;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DockerizedKafkaIntegrationTest {
    private static final String RECEIVER_TOPIC = "receiver.t";

    @Autowired
    private Receiver receiver;
    @Autowired
    private Sender sender;

    @Test
    public void testReceive() throws Exception {
        sender.send(RECEIVER_TOPIC, "Hello Spring Kafka!");

        receiver.getLatch().await(1000, TimeUnit.MILLISECONDS);
        // check that the message was received
        assertEquals(0, receiver.getLatch().getCount());
    }

}
