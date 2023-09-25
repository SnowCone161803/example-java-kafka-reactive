package com.example.kafka.reactive.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
@Slf4j
public class IntegrationTest {

    private final AtomicInteger eventCount = new AtomicInteger();

    @Autowired
    KafkaEventHandler kafkaEventHandler;

    @Test
    public void test_simple_handler() throws Exception {
        for (int i = 0; i < 9; ++i) {
            try {
                kafkaEventHandler.handle(createEvent());
            } catch (RuntimeException ex) {
                log.info("UNITTEST: event ended in exception", ex);
            }
        }
        kafkaEventHandler.handle(createEvent());
        kafkaEventHandler.handle(createEvent());
        kafkaEventHandler.handle(createEvent());
    }

    private KafkaEvent createEvent() {
        final int eventNumber = eventCount.getAndIncrement();
        return new KafkaEvent(eventNumber);
    }
}
