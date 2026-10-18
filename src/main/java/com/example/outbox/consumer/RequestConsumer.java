package com.example.outbox.consumer;

import com.example.outbox.config.AppProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Kafka entry point. The message value is irrelevant: the key is the request id, and the real data is
 * always read fresh from Aerospike.
 *
 * Why block() here: a Kafka listener thread is a plain thread owned by the listener container (not a
 * Netty / event-loop thread), and the container needs a synchronous result to decide between "commit
 * the offset" (method returned) and "retry / dead-letter" (method threw). Everything inside
 * RequestProcessor is non-blocking; only this one hand-off waits.
 *
 * Throughput = listener concurrency / time per record, so scale with topic partitions + concurrency.
 */
@Component
public class RequestConsumer {

    private final RequestProcessor processor;
    private final Duration blockTimeout;

    public RequestConsumer(RequestProcessor processor, AppProperties props) {
        this.processor = processor;
        // Same as the processing lease: if it takes this long, the claim is expiring anyway.
        this.blockTimeout = props.processor().lease();
    }

    @KafkaListener(topics = "${app.topic.name}")
    public void onMessage(ConsumerRecord<String, String> record) {
        String id = record.key();
        if (id == null || id.isBlank()) {
            // Not retryable (see KafkaConfig): goes straight to the dead-letter topic.
            throw new IllegalArgumentException("Message without a key at "
                    + record.topic() + "-" + record.partition() + "@" + record.offset());
        }
        processor.process(id).block(blockTimeout);
    }
}
