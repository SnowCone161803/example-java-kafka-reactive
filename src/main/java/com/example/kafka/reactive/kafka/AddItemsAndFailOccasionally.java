package com.example.kafka.reactive.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class AddItemsAndFailOccasionally {

    private static final int FAIL_EVERY = 3;
    private final KafkaEventHandler kafkaEventHandler;

    private final AtomicInteger failCount = new AtomicInteger();

    private Mono<Boolean> addItem(String id) {
        if (failCount.incrementAndGet() < FAIL_EVERY) {
            log.info("simulating adding succeeding: [{}]", id);
            return Mono.just(true);
        } else {
            failCount.set(0);
            return Mono.<Boolean>fromCallable(() -> {
                // add delay to allow other handlers to complete
                // you'll see some handlers complete, but others fail on the event that fails to complete
                Thread.sleep(500);
                log.info("simulating failure to add id: [{}]", id);
                throw new RuntimeException("Unable to add [" + id + "]");
                // needed because Thread.sleep(...) is blocking
            }).subscribeOn(Schedulers.boundedElastic());
        }
    }

    @PostConstruct
    public void addAddHandler() {
        log.info("adding add handler");
        kafkaEventHandler.addHandler(event -> event
            .map(KafkaEvent::getId)
            .flatMap(this::addItem));
    }
}
