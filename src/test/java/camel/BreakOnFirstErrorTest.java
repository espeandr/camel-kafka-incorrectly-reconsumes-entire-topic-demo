package camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.endpoint.dsl.DirectEndpointBuilderFactory.DirectEndpointBuilder;
import org.apache.camel.builder.endpoint.dsl.KafkaEndpointBuilderFactory.KafkaEndpointBuilder;
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
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@CamelSpringBootTest
@EnableAutoConfiguration
@UseAdviceWith
@EmbeddedKafka(controlledShutdown = true, partitions = 1)
public class BreakOnFirstErrorTest {

    private final List<String> consumedRecords = new ArrayList<>();

    @Autowired
    protected CamelContext camelContext;

    @Value("${spring.embedded.kafka.brokers}")
    private String brokerAddresses;
    @Autowired
    protected ProducerTemplate kafkaProducer;
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
        final List<String> producedRecords = List.of("1", "2", "3", "4", "5", "6"); // <- Error is thrown once on record "5"
        final List<String> expectedConsumedRecords = List.of("1", "2", "3", "4", "5", "5", "6"); // 5 should be consumed twice as error is thrown

        producedRecords.forEach(kafkaProducer::sendBody);

        await().untilAsserted(() ->
                                  // Assertion fails as all records on topic are reconsumed on error.
                                  assertThat(consumedRecords).isEqualTo(expectedConsumedRecords));
    }

    private void createConsumerRoute(RouteBuilder builder) {
        builder.from(kafkaTestTopic()
                         .autoOffsetReset("earliest")
                         .breakOnFirstError(true)
                         .maxPollRecords(4) // 1, 2, 4 causes the test to fail.
            )
            .process().body(String.class, body -> consumedRecords.add(body))
            .process(this::ifIsFifthRecordThrowException);
    }

    private void ifIsFifthRecordThrowException(Exchange e) {
        if (++consumptionCounter == 5) {
            throw new RuntimeException("ERROR_TRIGGERED_BY_TEST");
        }
    }

    private void createProducerRoute(RouteBuilder builder) {
        final DirectEndpointBuilder mockKafkaProducer = direct("mockKafkaProducer");
        kafkaProducer.setDefaultEndpoint(mockKafkaProducer.resolve(camelContext));

        builder.from(mockKafkaProducer)
            .to(kafkaTestTopic());
    }

    private KafkaEndpointBuilder kafkaTestTopic() {
        return kafka("test_topic")
            .brokers(brokerAddresses);
    }
}
