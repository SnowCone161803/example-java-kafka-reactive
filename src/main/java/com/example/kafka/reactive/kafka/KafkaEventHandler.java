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

    // way too long
    private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration STARTUP_TIMEOUT = Duration.ofMillis(100);

    // replay everything everytime it has been subscribed to
    private Sinks.Many<Mono<?>> handlerSink;
    private Sinks.One<KafkaEvent> eventSink;
    private Flux<Mono<?>> handlerFlux;
    private final Lock startupLock = new ReentrantLock();

    private final AtomicInteger eventCount = new AtomicInteger(0);

    public void handle(KafkaEvent event) {
        final int eventNumber = nextEventNumber();
        log.info("event {} starting", eventNumber);
        try {
            // should be way longer than needed
            if (!startupLock.tryLock(STARTUP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Unable to aquire lock when starting up event handler");
            }
            eventSink.tryEmitValue(event);
            // TODO: don't call this every time, try using Flux.transform(...) instead
            //       (this might mean that less of the `Flux` will need to be rebuilt for each event)
            handlerFlux
                .doOnSubscribe((s) -> {
                    this.startupLock.unlock();
                })
//                .flatMap(f -> f.apply(Mono.just(event)))
                .blockLast(EVENT_TIMEOUT);
            log.info("event {} complete", eventNumber);
        } catch (InterruptedException ex) {
            throw new IllegalStateException("event lock timed out", ex);
        } catch (Exception ex) {
            log.error("event {} failed", eventNumber, ex);
            throw ex;
        }
    }

    public void addHandler(Function<Mono<KafkaEvent>, Mono<?>> handler) {
        handlerSink.tryEmitNext(handler.apply(eventSink.asMono()));
    }

    private int nextEventNumber() {
        return eventCount.getAndIncrement();
    }

    @PostConstruct
    public void initialiseSink() {
        handlerSink = Sinks.many().replay().all();
        handlerFlux = handlerSink.asFlux();
        eventSink = Sinks.one();
    }

    /**
     * Complete the sink once all other handlers have been added.
     */
    @EventListener
    public void flagAllHandlersAdded(ContextRefreshedEvent event) {
        handlerSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
    }
}
