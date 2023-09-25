package com.example.kafka.reactive.kafka;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Class to handle a single event without reconstructing Mono / Flux instances each time.
 */
public class SingleEventHandler<E, R> {

    private final Sinks.One<E> eventSink = Sinks.one();
    private final Lock newEventLock = new ReentrantLock();
    private final Mono<R> fullAction;

    public static <T, R> SingleEventHandler<T, R> handleEventWith(Function<Mono<T>, Mono<R>> handler) {
        return new SingleEventHandler<T, R>(handler);
    }

    private SingleEventHandler(Function<Mono<E>, Mono<R>> handler) {
        // unlock the new event lock as soon as the handler is subscribed to
        // ASSUMPTION: this should be a FAST action
        // TODO: test with jcstress
        final Mono<E> unlockingMono = eventSink.asMono()
            .doOnSubscribe(e -> newEventLock.unlock());
        this.fullAction = handler.apply(unlockingMono);
    }

    public Mono<R> next(E event) {
        newEventLock.lock();
        eventSink.tryEmitValue(event);
        return fullAction;
    }
}
