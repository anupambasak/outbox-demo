package com.example.outbox.external;

import reactor.core.publisher.Mono;

/**
 * The seam to the third-party API. Replace the implementation (or just the URL in
 * application.yml) to talk to the real service.
 *
 * Contract of the returned Mono:
 *  - emits the response body (possibly empty string) on success,
 *  - errors with {@link PermanentApiException} when retrying cannot help (e.g. HTTP 4xx),
 *  - errors with {@link TransientApiException} when retrying may help (5xx, timeout, connection error).
 */
public interface ExternalApiClient {

    /**
     * @param idempotencyKey stable per request (the record id); sent so the remote side can
     *                       recognise a repeated call and not perform the action twice
     * @param jsonPayload    the stored request payload
     */
    Mono<String> call(String idempotencyKey, String jsonPayload);
}
