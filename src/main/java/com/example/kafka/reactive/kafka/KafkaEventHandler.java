package com.example.kafka.reactive.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

@Service
@Slf4j
public class KafkaEventHandler {

    private SingleEventHandler<KafkaEvent, ?> eventHandler;

    private final AtomicInteger eventCount = new AtomicInteger(0);

    public void handle(KafkaEvent event) {
        final int eventNumber = nextEventNumber();
        log.info("event {} starting", eventNumber);
        try {
            eventHandler
                .next(event)
                .block();
            log.info("event {} complete", eventNumber);
        } catch (Exception ex) {
            log.error("event {} failed", eventNumber, ex);
            throw ex;
        }
    }

    public <R> void addHandler(Function<Mono<KafkaEvent>, Mono<R>> handler) {
       eventHandler = SingleEventHandler.handleEventWith(handler);
    }

    private int nextEventNumber() {
        return eventCount.getAndIncrement();
    }
}
