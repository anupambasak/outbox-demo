package com.example.outbox.consumer;

import com.example.outbox.config.AppProperties;
import com.example.outbox.domain.ApiRequest;
import com.example.outbox.domain.ApiRequestRepository;
import com.example.outbox.external.ExternalApiClient;
import com.example.outbox.external.PermanentApiException;
import com.example.outbox.external.TransientApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Clock;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * The consumer-side logic, fully reactive:
 *
 *   load record  ->  dedup / lease check  ->  claim (CAS)  ->  call external API  ->  save outcome
 *
 * Dedup is the record's own status: a Kafka message for a COMPLETED or FAILED record is a duplicate
 * delivery and is dropped without calling the external API again.
 *
 * Outcomes of the external call:
 *   success              -> COMPLETED + response saved
 *   PermanentApiException -> FAILED + reason saved             (message acked, no retry)
 *   TransientApiException -> record released, exception rethrown (Kafka error handler retries, then
 *                            dead-letters and marks the record FAILED via markExhausted)
 */
@Service
public class RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RequestProcessor.class);

    /**
     * Records can change between our read and our write (publisher flipping RECEIVED to PUBLISHED, a
     * sweeper, a zombie consumer). On such a CAS conflict, re-read and try again a few times.
     */
    private static final Retry CAS_RETRY = Retry.max(3)
            .filter(OptimisticLockingFailureException.class::isInstance);

    private final ApiRequestRepository repository;
    private final ExternalApiClient externalApi;
    private final Clock clock;

    private final String instanceId = UUID.randomUUID().toString();
    private final long leaseMillis;

    public RequestProcessor(ApiRequestRepository repository,
                            ExternalApiClient externalApi,
                            Clock clock,
                            AppProperties props) {
        this.repository = repository;
        this.externalApi = externalApi;
        this.clock = clock;
        this.leaseMillis = props.processor().lease().toMillis();
    }

    /**
     * Handle one Kafka message. Completes empty when there is nothing (more) to do. Errors only for
     * conditions the Kafka error handler should retry.
     */
    public Mono<Void> process(String id) {
        return claim(id).flatMap(this::callExternalAndRecord);
    }

    /**
     * Called by the Kafka recoverer when in-process retries are used up: mark the record FAILED so it
     * does not sit in PUBLISHED forever. Leaves it alone if another worker is actively processing it.
     */
    public Mono<Void> markExhausted(String id, String reason) {
        return recordOutcome(
                id,
                r -> !r.isLeasedByOther(instanceId, clock.millis()),
                r -> r.fail("Retries exhausted: " + reason, clock.millis()));
    }

    // ---- step 1: claim ----------------------------------------------------------------------

    private Mono<ApiRequest> claim(String id) {
        return Mono.defer(() -> repository.findById(id))
                .switchIfEmpty(Mono.fromRunnable(() -> log.warn("Request {} not found, ignoring message", id)))
                .flatMap(this::tryClaim)
                .retryWhen(CAS_RETRY);
    }

    private Mono<ApiRequest> tryClaim(ApiRequest request) {
        long now = clock.millis();

        if (request.isTerminal()) {
            log.info("Request {} is already {}: duplicate delivery, skipping", request.getId(), request.getStatus());
            return Mono.empty();
        }
        if (request.isLeasedByOther(instanceId, now)) {
            // Someone else is working on it. If they die, their lease expires and the sweeper requeues it.
            log.info("Request {} is being processed by {}: skipping", request.getId(), request.getClaimedBy());
            return Mono.empty();
        }

        request.startProcessing(instanceId, now, leaseMillis);
        return repository.save(request);   // CAS: fails if the record changed since we read it
    }

    // ---- step 2: call the external API and record the outcome ---------------------------------

    private Mono<Void> callExternalAndRecord(ApiRequest claimed) {
        String id = claimed.getId();

        return externalApi.call(id, claimed.getPayload())
                .flatMap(response -> recordOutcome(
                        id,
                        r -> true,
                        r -> r.complete(response, clock.millis())))
                .onErrorResume(PermanentApiException.class, ex -> {
                    log.warn("Request {} rejected permanently: {}", id, ex.getMessage());
                    return recordOutcome(
                            id,
                            r -> true,
                            r -> r.fail(ex.getMessage(), clock.millis()));
                })
                .onErrorResume(TransientApiException.class, ex -> {
                    log.warn("Request {} hit a transient error: {}", id, ex.getMessage());
                    return recordOutcome(
                                    id,
                                    r -> instanceId.equals(r.getClaimedBy()),   // only release our own claim
                                    r -> r.releaseForRetry(ex.getMessage(), clock.millis()))
                            .then(Mono.<Void>error(ex));                       // let Kafka retry
                });
    }

    /**
     * Re-read the record, apply a mutation if it is still pending and {@code applicable}, and save with
     * CAS (retrying on conflict). Used for outcomes, where we want the result written even if the
     * record's version moved on while the external call was in flight.
     */
    private Mono<Void> recordOutcome(String id,
                                     Predicate<ApiRequest> applicable,
                                     Consumer<ApiRequest> mutation) {
        return Mono.defer(() -> repository.findById(id))
                .filter(r -> !r.isTerminal() && applicable.test(r))
                .flatMap(r -> {
                    mutation.accept(r);
                    return repository.save(r);
                })
                .retryWhen(CAS_RETRY)
                .then();
    }
}
