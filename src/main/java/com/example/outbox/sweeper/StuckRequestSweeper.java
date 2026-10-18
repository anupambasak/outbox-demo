package com.example.outbox.sweeper;

import com.example.outbox.config.AppProperties;
import com.example.outbox.domain.ApiRequest;
import com.example.outbox.domain.ApiRequestRepository;
import com.example.outbox.domain.Status;
import com.example.outbox.support.PollingLoop;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;

/**
 * Safety net for records that no healthy worker is moving forward:
 *
 *  - PROCESSING with an expired lease: the consumer that claimed it died. Requeue it.
 *  - PUBLISHED for too long: the Kafka message was lost, dead-lettered by hand, or expired. Requeue it.
 *
 * "Requeue" = set RECEIVED, so the publisher sends the id to Kafka again. A duplicate message is
 * harmless: the consumer drops it if the record is already COMPLETED / FAILED.
 *
 * A record claimed too many times (e.g. a payload that crashes the consumer every time) is marked
 * FAILED instead of being requeued forever.
 */
@Component
@ConditionalOnProperty(name = "app.sweeper.enabled", havingValue = "true", matchIfMissing = true)
public class StuckRequestSweeper {

    private static final Logger log = LoggerFactory.getLogger(StuckRequestSweeper.class);

    private final ApiRequestRepository repository;
    private final Clock clock;
    private final AppProperties.Sweeper config;

    private Disposable loop;

    public StuckRequestSweeper(ApiRequestRepository repository, Clock clock, AppProperties props) {
        this.repository = repository;
        this.clock = clock;
        this.config = props.sweeper();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        log.info("Stuck-request sweeper started (every {})", config.interval());
        loop = PollingLoop.start("stuck-request-sweeper", config.interval(), this::sweepOnce);
    }

    @PreDestroy
    public void stop() {
        if (loop != null) {
            loop.dispose();
        }
    }

    Mono<Void> sweepOnce() {
        long now = clock.millis();
        long publishedCutoff = now - config.publishedStuckAfter().toMillis();

        Flux<ApiRequest> expiredProcessing = repository.findByStatus(Status.PROCESSING)
                .filter(r -> r.getClaimUntil() < now);
        Flux<ApiRequest> stalePublished = repository.findByStatus(Status.PUBLISHED)
                .filter(r -> r.getUpdatedAt() < publishedCutoff);

        return Flux.concat(expiredProcessing, stalePublished)
                .concatMap(this::recover)
                .then();
    }

    private Mono<ApiRequest> recover(ApiRequest request) {
        long now = clock.millis();

        if (request.getAttempts() >= config.maxAttempts()) {
            log.warn("Request {} stuck in {} after {} attempts: marking FAILED",
                    request.getId(), request.getStatus(), request.getAttempts());
            request.fail("Gave up after " + request.getAttempts() + " attempts without completing", now);
        } else {
            log.info("Request {} stuck in {} (attempt {}): requeueing",
                    request.getId(), request.getStatus(), request.getAttempts());
            request.requeue(now);
        }

        // CAS: if a worker moved the record while we were looking, it was not stuck after all.
        return repository.save(request)
                .onErrorResume(OptimisticLockingFailureException.class, e -> Mono.empty());
    }
}
