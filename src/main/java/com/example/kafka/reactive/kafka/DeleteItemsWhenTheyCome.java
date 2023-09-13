package com.example.kafka.reactive.kafka;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeleteItemsWhenTheyCome {

    private final KafkaEventHandler kafkaEventhandler;

    private Mono<Boolean> deleteItem(int id) {
        log.info("event triggered deletion of item with id [{}]", id);
        return Mono.just(true);
    }

    @PostConstruct
    public void addDeleteHandler() {
        log.info("adding delete handler");
        kafkaEventhandler
            .addHandler(event -> event
                .map(KafkaEvent::getId)
                .map(this::deleteItem));
    }
}
