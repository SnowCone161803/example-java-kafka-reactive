package com.example.kafka.reactive.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Service
@Slf4j
public class KafkaEventHandler {

    // way too long
    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(5);

    // replay everything everytime it has been subscribed to
    private Sinks.Many<Function<Mono<KafkaEvent>, Mono<?>>> handlerSink;

    private final AtomicInteger eventCount = new AtomicInteger(0);


    public void handle(KafkaEvent event) {
        final int eventNumber = nextEventNumber();
        log.info("event {} starting", eventNumber);
        try {
            // TODO: don't call this every time
            handlerSink
                .asFlux()
                .flatMap((f) -> f.apply(Mono.just(event)))
                .blockLast(EVENT_TIMEOUT);
            log.info("event {} complete", eventNumber);
        } catch (Exception ex) {
            log.error("event {} failed", eventNumber, ex);
            throw ex;
        }
    }

    public void addHandler(Function<Mono<KafkaEvent>, Mono<?>> handler) {
        handlerSink.tryEmitNext(handler);
    }

    private int nextEventNumber() {
        return this.eventCount.getAndIncrement();
    }

    @PostConstruct
    public void initialiseSink() {
        handlerSink = Sinks.many().replay().all();
    }

    /**
     * Complete the sink once all other handlers have been added.
     */
    @EventListener
    public void flagAllHandlersAdded(ContextRefreshedEvent event) {
        handlerSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
    }
}
