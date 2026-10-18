package com.example.outbox.config;

import com.example.outbox.consumer.RequestProcessor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.time.Duration;

@Configuration
public class KafkaConfig {

    /** Created on startup by Spring's KafkaAdmin if it does not exist yet. */
    @Bean
    NewTopic requestsTopic(AppProperties props) {
        return TopicBuilder.name(props.topic().name())
                .partitions(props.topic().partitions())
                .replicas(props.topic().replicas())
                .build();
    }

    /**
     * Dead-letter topic. DeadLetterPublishingRecoverer publishes to the SAME partition number as the
     * original record, so this topic needs at least as many partitions as the main one.
     */
    @Bean
    NewTopic requestsDeadLetterTopic(AppProperties props) {
        return TopicBuilder.name(props.topic().name() + ".DLT")
                .partitions(props.topic().partitions())
                .replicas(props.topic().replicas())
                .build();
    }

    /**
     * Picked up automatically by Spring Boot's default listener container factory.
     *
     * A listener exception is retried in-process with exponential backoff (1s, 2s, 4s, 8s, 15s ...).
     * When retries run out, the recoverer (1) marks the Aerospike record FAILED and (2) publishes the
     * Kafka message to "<topic>.DLT". The offset is then committed and the partition moves on.
     *
     * Note: while a record is being retried its partition is paused (blocking retry). That is fine for
     * moderate volume; use non-blocking retry topics if one slow record must not delay the others.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                          RequestProcessor processor,
                                          AppProperties props) {

        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(kafkaTemplate);

        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            if (record.key() instanceof String id) {
                String reason = NestedExceptionUtils.getMostSpecificCause(exception).toString();
                processor.markExhausted(id, reason).block(Duration.ofSeconds(15));
            }
            deadLetters.accept(record, exception);
        };

        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(props.processor().maxRetries());
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(15_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // A message without a key can never succeed, so do not retry it.
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }
}
