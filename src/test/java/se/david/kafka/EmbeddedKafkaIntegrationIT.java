package se.david.kafka;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;
import se.david.kafka.consumer.Receiver;
import se.david.kafka.producer.Sender;


@RunWith(SpringRunner.class)
@SpringBootTest
public class EmbeddedKafkaIntegrationIT {
    private static final String RECEIVER_TOPIC = "receiver.t";

    @Autowired
    private Sender sender;

    @Autowired
    private Receiver receiver;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(
            1,
            true,
            RECEIVER_TOPIC
    );

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String kafkaBootstrapServers = embeddedKafka.getBrokersAsString();

        // override the property in application.properties
        System.setProperty("kafka.bootstrap-servers", kafkaBootstrapServers);
    }

    @Before
    public void setUp() throws Exception {
        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(
                    messageListenerContainer,
                    embeddedKafka.getPartitionsPerTopic()
            );
        }
    }

    @Test
    public void testReceive() throws Exception {
        String message = "Hello Spring Kafka!";

        sender.send(RECEIVER_TOPIC, message);

        await()
                .pollDelay(1, TimeUnit.MILLISECONDS)
                .pollInterval(1, TimeUnit.MILLISECONDS)
                .atMost(100, TimeUnit.MILLISECONDS)
                .until(receiver::getReceivedMessage, is(message));
    }
}
