# example-java-kafka-reactive
Reactive wrapper around a kafka event handler

This app doesn't run. Run `IntegrationTests` and see what happens in the log.

It has been setup so that every event triggers both:
- an add operation (simulated with log message)
- a delete operation (simulated with log message)

The add operation fails every 3 adds.  
This shows that failures throw an exception back to kafka to let it handle things again  
This allows kafka to retry the event.

The log also shows that even though previous events fail, future events are handled normally.

## Parts of the system

`IntegrationTests`  
- not quite kafka integration tests
- shows what happens with repeated calls
- TODO: set this up to work with Kafka
- ASSUMPTION: kafka only processes one event at a time (events from different partitions do not matter)

`KafkaEventHandler`  
- Accepts events from kafka (simulated in the integration test)
- passes the event to all handlers
- can accept handlers from other services' `@PostConstruct`

`AddItemsAndFailOccasionally`  
- adds a handler to `KafkaEventHandler`
- logs out that it is adding
- fails occasionally (simulated)

`DeleteItemsWhenTheyCome`
- adds a handler to `KafkaEventHandler`
- logs out that it is deleting

## Notes

adding multiple handlers this way can mean that some handlers can succeed and others fail.  
I'd like to see use cases to work out how to handle situations individually