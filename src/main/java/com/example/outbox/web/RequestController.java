package com.example.outbox.web;

import com.example.outbox.domain.ApiRequest;
import com.example.outbox.domain.ApiRequestRepository;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Clock;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Entry point. POST does exactly one thing: one atomic Aerospike write of a RECEIVED record, then
 * 202. Everything else (publishing, calling the external API) happens asynchronously.
 */
@RestController
@RequestMapping("/requests")
public class RequestController {

    /** The key becomes the record id, the Kafka key and an HTTP header downstream, so keep it tame. */
    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z0-9._-]{1,128}");

    private final ApiRequestRepository repository;
    private final Clock clock;

    public RequestController(ApiRequestRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * Accepts any JSON document as the payload.
     *
     * Optional "Idempotency-Key" header: if the client retries the POST with the same key, no second
     * record is created; the first one is returned (200). Same key with a different body is a 409.
     */
    @PostMapping
    public Mono<ResponseEntity<ApiRequestView>> create(
            @RequestBody JsonNode body,
            @RequestHeader(name = "Idempotency-Key", required = false) String idempotencyKey) {

        if (body == null || body.isNull() || body.isMissingNode()) {
            return Mono.error(new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Request body must be a JSON document"));
        }

        String key = idempotencyKey == null ? "" : idempotencyKey.trim();
        if (!key.isEmpty() && !KEY_PATTERN.matcher(key).matches()) {
            return Mono.error(new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Idempotency-Key must match " + KEY_PATTERN.pattern()));
        }

        String payload = body.toString();

        if (key.isEmpty()) {
            return insert(UUID.randomUUID().toString(), payload);
        }

        // Client supplied a key: replay if we already have it, otherwise create it.
        return repository.findById(key)
                .map(existing -> replay(existing, payload))
                .switchIfEmpty(Mono.defer(() -> insertWithKey(key, payload)));
    }

    @GetMapping("/{id}")
    public Mono<ApiRequestView> get(@PathVariable String id) {
        return repository.findById(id)
                .map(ApiRequestView::from)
                .switchIfEmpty(Mono.error(new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Request not found: " + id)));
    }

    // ------------------------------------------------------------------------------------

    private Mono<ResponseEntity<ApiRequestView>> insert(String id, String payload) {
        ApiRequest entity = ApiRequest.received(id, payload, clock.millis());
        // version == 0 means "new": the save is create-only, it can never overwrite an existing record.
        return repository.save(entity).map(this::accepted);
    }

    /** Insert, but if a concurrent request with the same key won the race, replay that one instead. */
    private Mono<ResponseEntity<ApiRequestView>> insertWithKey(String id, String payload) {
        return insert(id, payload)
                .onErrorResume(
                        e -> e instanceof DuplicateKeyException || e instanceof OptimisticLockingFailureException,
                        e -> repository.findById(id).map(existing -> replay(existing, payload)));
    }

    private ResponseEntity<ApiRequestView> accepted(ApiRequest saved) {
        return ResponseEntity.accepted()
                .location(URI.create("/requests/" + saved.getId()))
                .body(ApiRequestView.from(saved));
    }

    private ResponseEntity<ApiRequestView> replay(ApiRequest existing, String payload) {
        if (!payload.equals(existing.getPayload())) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT, "Idempotency-Key already used with a different payload");
        }
        return ResponseEntity.ok()
                .location(URI.create("/requests/" + existing.getId()))
                .body(ApiRequestView.from(existing));
    }
}
