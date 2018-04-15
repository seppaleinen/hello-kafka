package se.david.kafka;

import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import se.david.kafka.consumer.Receiver;
import se.david.kafka.producer.Sender;

import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.verify;

@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = Application.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@DirtiesContext
@ActiveProfiles("test")
@EmbeddedKafka(topics = "receiver.t", controlledShutdown = true, count = 1)
public class IntegrationTest {
    @LocalServerPort
    private int port;
    @SpyBean
    private Sender sender;
    @SpyBean
    private Receiver receiver;
    @Value("${topic.receiver}")
    private String topic;

    @BeforeEach
    void setup() throws Exception {
        RestAssured.port = port;
    }

    @Test
    void test() {
        given().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).
                when().get("/kafka/message")
                .then()
                .statusCode(HttpStatus.OK.value());

        verify(sender).send(topic, "message");

        await().atMost(500, TimeUnit.MILLISECONDS)
                .until(receiver::getReceivedMessage, is("message"));
    }

    @Test
    void testReceive() {
        String message = "message";

        sender.send(topic, message);

        await()
                .pollDelay(50, TimeUnit.MILLISECONDS)
                .pollInterval(1, TimeUnit.MILLISECONDS)
                .atMost(100, TimeUnit.MILLISECONDS)
                .until(receiver::getReceivedMessage, is(message));
    }
}
