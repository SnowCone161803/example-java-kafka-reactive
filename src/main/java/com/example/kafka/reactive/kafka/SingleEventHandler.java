package com.example.kafka.reactive.kafka;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Class to handle a single event without reconstructing Mono / Flux instances each time.
 */
@Slf4j
public class SingleEventHandler<E, R> {

    private final Sinks.Many<E> eventSink = Sinks.many().multicast().onBackpressureBuffer();
    private final Lock newEventLock = new ReentrantLock();
    private final Flux<R> fullAction;

    public static <T, R> SingleEventHandler<T, R> handleEventWith(Function<Flux<T>, Flux<R>> handler) {
        return new SingleEventHandler<>(handler);
    }

    private SingleEventHandler(Function<Flux<E>, Flux<R>> handler) {
        // unlock the new event lock as soon as the handler is subscribed to
        // ASSUMPTION: this should be a FAST action
        // TODO: test with jcstress
        this.eventSink.asFlux().subscribe();
        final var unlockingFlux = eventSink.asFlux()
            .doOnSubscribe(e -> {
                log.info("inner doOnSubscribe hit");
                newEventLock.unlock();
            });
        this.fullAction = handler.apply(unlockingFlux);
    }

    public Mono<R> next(E event) {
        // lock so that the result of the returned mono is the result of the input event
        // TODO: test this is necessary (jcstress)
        newEventLock.lock();
        Mono<R> nextValue = fullAction.next();
        eventSink.tryEmitNext(event);
        return nextValue;
    }
}
