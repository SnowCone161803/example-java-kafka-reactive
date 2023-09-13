package com.example.kafka.reactive.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Service
@Slf4j
public class KafkaHandler {

    // way too long
    private static final Duration EVENT_TIMEOUT = Duration.ofMinutes(5);

    // replay everything everytime it has been subscribed to
    private final Sinks.Many<Function<Mono<KafkaEvent>, Mono<?>>> handlerSink = Sinks.many().replay().all();

    private final AtomicInteger eventCount = new AtomicInteger(0);

    public void handle(KafkaEvent event) {
        final int eventNumber = nextEventNumber();
        log.info("event {} starting", eventNumber);
        try {
            var calledHandlers = handlerSink
                .asFlux()
                .map((f) -> f.apply(Mono.just(event)));
            Flux.merge(calledHandlers)
                .blockLast(EVENT_TIMEOUT);
            log.info("event {} complete", eventNumber);
        } catch (Exception ex) {
            log.error("event {} failed", eventNumber);
        }
    }

    public void addHandler(Function<Mono<KafkaEvent>, Mono<?>> handler) {
        handlerSink.tryEmitNext(handler);
    }

    private int nextEventNumber() {
        return this.eventCount.getAndIncrement();
    }

    /**
     * Connect sink as the last thing that happens to prevent events from not being handled by all
     */
    @PostConstruct
    // TODO: find how to get the last order
    @Order(Integer.MAX_VALUE)
    public void connectSink() {
        // TODO: connect the handler flux as all handlers will be added at this point
    }
}
