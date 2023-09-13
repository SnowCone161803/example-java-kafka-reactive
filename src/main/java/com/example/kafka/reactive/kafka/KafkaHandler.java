package com.example.kafka.reactive.kafka;

import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.function.Function;

@Service
public class KafkaHandler {

    // way too long
    private static final Duration EVENT_TIMEOUIT = Duration.ofMinutes(5);

    // replay everything everytime it has been subscribed to
    private final Sinks.Many<Function<Mono<KafkaEvent>, Mono<?>>> handlerSink = Sinks.many().replay().all();

    public void handle(KafkaEvent event) {
        var calledHandlers = handlerSink
            .asFlux()
            .map((f) -> f.apply(Mono.just(event)));
        Flux.merge(calledHandlers)
            .blockLast(Duration.ofDays(1));
    }

    public void addHandler(Function<Mono<KafkaEvent>, Mono<?>> handler) {
        handlerSink.tryEmitNext(handler);
    }

    /**
     * Connect sink as the last thing that happens to prevent events from not being handled by all
     */
    @PostConstruct
    // TODO: find how to get the last order
    @Order(Integer.MAX_VALUE)
    public void connectSink() {
    }
}
