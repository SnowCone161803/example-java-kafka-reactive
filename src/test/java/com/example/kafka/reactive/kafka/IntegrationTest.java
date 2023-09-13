package com.example.kafka.reactive.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
public class IntegrationTest {

    private final AtomicInteger eventCount = new AtomicInteger();

    @Autowired
    KafkaEventHandler kafkaEventHandler;

    @Test
    public void test_simple_hander() throws Exception {
        kafkaEventHandler.handle(createEvent());
    }

    private KafkaEvent createEvent() {
        final int eventNumber = eventCount.getAndIncrement();
        return new KafkaEvent(eventNumber);
    }
}
