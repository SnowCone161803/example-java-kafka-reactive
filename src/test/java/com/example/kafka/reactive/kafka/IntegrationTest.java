package com.example.kafka.reactive.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.text.MessageFormat;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
@Slf4j
public class IntegrationTest {

    private final AtomicInteger eventCount = new AtomicInteger();

    private final Queue<String> eventsHandled = new ConcurrentLinkedQueue<>();

    @Autowired
    KafkaEventHandler kafkaEventHandler;

    @BeforeEach
    public void setup() {
        kafkaEventHandler.addHandler(e -> e
            .map(KafkaEvent::getId)
            .doOnNext(eventsHandled::add)
            .doOnNext(e2 -> log.info("event appeared: {}", e2)));
    }

    @Test
    public void test_simple_handler() throws Exception {
        var pool = Executors.newFixedThreadPool(20);
        pool.execute(() -> sendEvents("first"));
        pool.execute(() -> sendEvents("second"));
        pool.execute(() -> sendEvents("third"));
        pool.execute(() -> sendEvents("fourth"));
        if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
            displayResults();
            throw new RuntimeException("UNITTEST: timed out waiting for pool to terminate");
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            displayResults();
            throw new RuntimeException(e);
        }
        displayResults();
    }

    private void displayResults() {
        eventsHandled.stream()
            .filter(e -> e.startsWith("first"))
            .forEach(e -> log.info("first event: {}", e));
        log.info("results: {}", eventsHandled);
    }

    private void sendEvents(String discriminator) {
        for (int i = 0; i < 9; ++i) {
            try {
                kafkaEventHandler.handle(createEvent(discriminator));
            } catch (RuntimeException ex) {
                log.info("UNITTEST: event ended in exception", ex);
            }
        }
    }

    private KafkaEvent createEvent(String discriminator) {
        final int eventNumber = eventCount.getAndIncrement();
        return new KafkaEvent(MessageFormat.format(
            "{0}-{1}",
            discriminator,
            eventNumber));
    }
}
