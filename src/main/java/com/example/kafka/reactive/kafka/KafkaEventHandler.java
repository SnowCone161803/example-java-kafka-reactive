package com.example.kafka.reactive.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

@Service
@Slf4j
public class KafkaEventHandler {

    private SingleEventHandler<KafkaEvent, ?> eventHandler;
    private final Lock eventLock = new ReentrantLock();

    private final AtomicInteger eventCount = new AtomicInteger(0);

    public void handle(KafkaEvent event) {
        final int eventNumber = nextEventNumber();
        log.info("event {} starting", eventNumber);
        try {
            eventLock.lock();
            eventHandler
                .next(event)
                .doOnSubscribe(e -> {
                    log.info("outer doOnSubscribe hit");
                    eventLock.unlock();
                })
                .block();
            log.info("event {} complete", eventNumber);
        } catch (Exception ex) {
            log.error("event {} failed", eventNumber, ex);
            throw ex;
        }
    }

    public <R> void addHandler(Function<Flux<KafkaEvent>, Flux<R>> handler) {
       eventHandler = SingleEventHandler.handleEventWith(handler);
    }

    private int nextEventNumber() {
        return eventCount.getAndIncrement();
    }
}
