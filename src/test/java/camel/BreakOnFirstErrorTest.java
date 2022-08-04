package camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.endpoint.dsl.DirectEndpointBuilderFactory;
import org.apache.camel.builder.endpoint.dsl.DirectEndpointBuilderFactory.DirectEndpointBuilder;
import org.apache.camel.builder.endpoint.dsl.KafkaEndpointBuilderFactory;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.ArrayList;
import java.util.List;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.direct;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.kafka;
import static org.apache.camel.component.kafka.KafkaConstants.MANUAL_COMMIT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@CamelSpringBootTest
@EnableAutoConfiguration
@UseAdviceWith
@EmbeddedKafka(controlledShutdown = true, partitions = 1)
public class BreakOnFirstErrorTest {

    private final List<String> consumedRecords = new ArrayList<>();
    @Autowired
    protected ProducerTemplate kafkaInputProducer;
    @Autowired
    protected CamelContext camelContext;
    @Value("${spring.embedded.kafka.brokers}")
    private String brokerAddresses;
    private int consumptionCounter = 0;

    @BeforeEach
    public void setupTestRoutes() throws Exception {
        AdviceWithRouteBuilder.addRoutes(camelContext, builder -> {
            createProducerRoute(builder);
            createConsumerRoute(builder);
        });
        camelContext.start();
    }

    @Test
    public void shouldOnlyReconsumeFailedMessageOnError() {
        final List<String> recordsOnKafka = List.of("1", "2", "3", "4", "5", "6"); // <- Error is thrown on record "5", and should be reconsumed.
        final List<String> expectedConsumedRecords = List.of("1", "2", "3", "4", "5", "5", "6"); // 5 should be consumed twice as error is thrown

        recordsOnKafka.forEach(recordToProduce -> kafkaInputProducer.sendBody(recordToProduce));

        await().until(() -> consumedRecords.size() >= expectedConsumedRecords.size());
        assertThat(consumedRecords).isEqualTo(expectedConsumedRecords);
        // Test fails as all records on topic is reconsumed on error.
    }

    private void createConsumerRoute(RouteBuilder builder) {
        final DirectEndpointBuilderFactory.DirectEndpointBuilder commitOffsetRoute = direct("commit-offset-route");
        builder.from(kafkaTestTopic()
                         .autoOffsetReset("earliest")
                         .allowManualCommit(true)
                         .autoCommitEnable(false)
                         .breakOnFirstError(true)
                         .maxPollRecords(1) // <- Setting this to 1 causes camel to incorrectly reconsume all messages on error
            )
            .process().body(String.class, body -> consumedRecords.add(body))
            .process(this::ifIsFifthRecordThrowException)
            .process(this::commitOffsetManually);

    }

    private void commitOffsetManually(final Exchange e) {
        e.getIn().getHeader(MANUAL_COMMIT, KafkaManualCommit.class).commit();
    }

    private void ifIsFifthRecordThrowException(Exchange e) {
        consumptionCounter++;
        if (consumptionCounter == 5) {
            throw new RuntimeException("ERROR_TRIGGER_BY_TEST");
        }
    }

    private void createProducerRoute(RouteBuilder builder) {
        DirectEndpointBuilder mockKafkaProducer = direct("mockKafkaProducer");
        kafkaInputProducer.setDefaultEndpoint(mockKafkaProducer.resolve(camelContext));

        builder.from(mockKafkaProducer)
            .to(kafkaTestTopic());
    }

    private KafkaEndpointBuilderFactory.KafkaEndpointBuilder kafkaTestTopic() {
        return kafka("test_topic")
            .brokers(brokerAddresses);
    }
}
