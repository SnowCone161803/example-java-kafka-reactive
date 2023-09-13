package com.example.kafka.reactive.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeleteItemsWhenTheyCome {

    private final KafkaEventHandler kafkaEventHandler;

    private Mono<Boolean> deleteItem(int id) {
        log.info("event triggered deletion of item with id [{}]", id);
        return Mono.just(true);
    }

    @PostConstruct
    public void addDeleteHandler() {
        log.info("adding delete handler");
        kafkaEventHandler.addHandler(event -> event
            .map(KafkaEvent::getId)
            .flatMap(this::deleteItem));
    }
}
