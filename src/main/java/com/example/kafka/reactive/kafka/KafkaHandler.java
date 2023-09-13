package com.example.kafka.reactive.kafka;

import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Sinks;

import javax.annotation.PostConstruct;
import java.util.function.Function;

@Service
public class KafkaHandler {

    private final Sinks.One<?> eventHandlerSink = Sinks.one();

    // replay everything everytime it has been subscribed to
    private final Sinks.Many<Function<KafkaEvent, ?>> handlerSink = Sinks.many().replay().all();

    public void handle(KafkaEvent event) {
    }

    public void addHandler(Function<KafkaEvent, ?> handler) {
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
