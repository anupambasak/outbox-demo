package com.example.outbox.publisher;

import com.example.outbox.config.AppProperties;
import com.example.outbox.domain.ApiRequest;
import com.example.outbox.domain.ApiRequestRepository;
import com.example.outbox.domain.Status;
import com.example.outbox.support.PollingLoop;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;

/**
 * The relay: moves RECEIVED records from Aerospike (the outbox) to Kafka.
 *
 * Per record:  claim (CAS save with a short lease)  ->  send to Kafka and wait for the ack
 *              ->  mark PUBLISHED (CAS save).
 *
 * The CAS claim is what replaces "SELECT ... FOR UPDATE SKIP LOCKED": of two instances that read
 * the same record, only one save succeeds, the other gets OptimisticLockingFailureException and skips.
 *
 * Delivery is at-least-once. A crash between the Kafka ack and the PUBLISHED write means the record
 * is published again once the lease expires. The consumer deduplicates on the record's status.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final ApiRequestRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Clock clock;

    private final String instanceId = UUID.randomUUID().toString();
    private final String topic;
    private final Duration pollInterval;
    private final int batchSize;
    private final int concurrency;
    private final long claimLeaseMillis;
    private final Duration sendTimeout;

    private Disposable loop;

    public OutboxPublisher(ApiRequestRepository repository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           Clock clock,
                           AppProperties props) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
        this.topic = props.topic().name();
        this.pollInterval = props.publisher().pollInterval();
        this.batchSize = props.publisher().batchSize();
        this.concurrency = props.publisher().concurrency();
        this.claimLeaseMillis = props.publisher().claimLease().toMillis();
        this.sendTimeout = props.publisher().sendTimeout();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        log.info("Outbox publisher {} started (poll every {}, batch {})", instanceId, pollInterval, batchSize);
        loop = PollingLoop.start("outbox-publisher", pollInterval, this::pollOnce);
    }

    @PreDestroy
    public void stop() {
        if (loop != null) {
            loop.dispose();
        }
    }

    /** One poll: find RECEIVED records without a live lease and publish up to batchSize of them. */
    Mono<Void> pollOnce() {
        long now = clock.millis();
        return repository.findByStatus(Status.RECEIVED)
                .filter(r -> r.getClaimUntil() < now)
                .take(batchSize)
                .flatMap(this::claimAndPublish, concurrency)
                .then();
    }

    private Mono<Void> claimAndPublish(ApiRequest request) {
        request.claimForPublish(instanceId, clock.millis(), claimLeaseMillis);

        return repository.save(request)
                // Another instance claimed it between our read and our write: not our record.
                .onErrorResume(OptimisticLockingFailureException.class, e -> {
                    log.debug("Request {} claimed by another publisher, skipping", request.getId());
                    return Mono.empty();
                })
                .flatMap(this::sendAndMarkPublished)
                // Any other failure: leave the record RECEIVED. Its lease expires and a later poll retries.
                .onErrorResume(e -> {
                    log.warn("Publishing request {} failed ({}); will retry after the lease expires",
                            request.getId(), e.toString());
                    return Mono.empty();
                });
    }

    private Mono<Void> sendAndMarkPublished(ApiRequest claimed) {
        String id = claimed.getId();

        // KafkaProducer.send() can block while it fetches metadata, so subscribe on boundedElastic.
        return Mono.defer(() -> Mono.fromFuture(kafkaTemplate.send(topic, id, id)))
                .subscribeOn(Schedulers.boundedElastic())
                .timeout(sendTimeout)
                // The future completes on the Kafka producer's I/O thread; hop off it before the Aerospike write.
                .publishOn(Schedulers.parallel())
                .flatMap(sendResult -> {
                    claimed.markPublished(clock.millis());
                    return repository.save(claimed);
                })
                // The consumer is quick: it may already have moved the record to PROCESSING before we got
                // here. Then our PUBLISHED write loses the CAS, which is correct and harmless.
                .onErrorResume(OptimisticLockingFailureException.class, e -> {
                    log.debug("Request {} already advanced by the consumer", id);
                    return Mono.empty();
                })
                .then();
    }
}
