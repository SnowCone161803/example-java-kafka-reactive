package com.example.kafka.reactive.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MonoFromDirectTest {

    @Test
    public void test_share_next_gives_me_things_multiple_times() throws Exception {
        // Does what I need
        var sink = Sinks.many().multicast().onBackpressureBuffer();

        // make the flux a hot flux
        sink.asFlux().subscribe();

        // get ready to catch the first value
        var first = sink.asFlux().next().share();

        // perform action on first value
        first.subscribe(v -> log.info("first: {}", v));
        first.subscribe(v -> log.info("first: {}", v));
        sink.tryEmitNext("alpha");

        // get ready to catch the second value
        var second = sink.asFlux().next().share();
        second.subscribe(v -> log.info("second: {}", v));
        second.subscribe(v -> log.info("second: {}", v));
        sink.tryEmitNext("beta");

        first.subscribe(v -> log.info("final first: {}", v));
        second.subscribe(v -> log.info("final second: {}", v));

        Thread.sleep(1000);
    }

    @Test
    public void test_getting_next_multiple_times_gives_values() throws Exception {
        // can't get this to work
        var sink = Sinks.many().multicast().onBackpressureBuffer();
        var flux = sink.asFlux().share();
        var first = flux.next().share();
        first.subscribe(v -> log.info("first: {}", v));
        first.subscribe(v -> log.info("first: {}", v));
        sink.tryEmitNext("alpha");

        Thread.sleep(1000);

        // nothing below here displayed
        var second = flux.next().share();
        second.subscribe(v -> log.info("second: {}", v));
        second.subscribe(v -> log.info("second: {}", v));
        sink.tryEmitNext("beta");

        Thread.sleep(1000);
    }

    @Test
    public void test_callable_gives_events_that_can_be_handled_multiple_times() throws Exception {
        var event = new AtomicInteger();
        var events = Mono.fromCallable(event::getAndIncrement).repeat();
        var first = events.next().share();
        var second = events.next().share();

        first.subscribe(v -> log.info("first: {}", v));
        second.subscribe(v -> log.info("second: {}", v));
        first.subscribe(v -> log.info("first: {}", v));
        second.subscribe(v -> log.info("second: {}", v));
    }

    @Test
    public void test_gettingNextValue_andSubscribingManyTimes() throws Exception {
        var sink = Sinks.one();
        var mono = sink.asMono();
        sink.tryEmitValue(1);
        sink.tryEmitValue(2);
        mono.subscribe(v -> log.info("mono value: {}", v));
        mono.subscribe(v -> log.info("mono value: {}", v));
        Thread.sleep(1000);
    }

    @Test
    public void can_sink_return_a_mono_I_can_repeat_with() throws Exception {
        AtomicInteger i = new AtomicInteger();
        var mono = Mono.fromCallable(() -> i.getAndIncrement());
        var events = mono.repeat();
        var first = events.next().share();
        var second = events.next().share();
        first.subscribe(v -> log.info("first event: {}", v));
        second.subscribe(v -> log.info("second event: {}", v));
        first.subscribe(v -> log.info("first event: {}", v));
        second.subscribe(v -> log.info("second event: {}", v));
        Thread.sleep(100);
    }
}
