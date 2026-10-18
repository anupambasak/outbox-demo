package com.example.outbox.external;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DEMO ONLY - stands in for the third-party API so the project runs end to end.
 * Enabled by app.external.mock-enabled=true; delete this class for production.
 *
 * Behaviour is chosen by the "simulate" field in the request payload:
 *   (absent) / "ok"  -> 200 after ~200 ms
 *   "bad"            -> 422 (permanent failure)
 *   "flaky"          -> 503 for the first two calls per Idempotency-Key, then 200
 *   "slow"           -> responds after 30 s, longer than the client timeout (transient timeout)
 *
 * Like a well-behaved real API, it honours Idempotency-Key: a repeated key returns the first result.
 */
@RestController
@RequestMapping("/mock-external")
@ConditionalOnProperty(name = "app.external.mock-enabled", havingValue = "true")
public class MockExternalApiController {

    private final ObjectMapper mapper;
    private final Map<String, JsonNode> completed = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> flakyCalls = new ConcurrentHashMap<>();

    public MockExternalApiController(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @PostMapping("/process")
    public Mono<ResponseEntity<JsonNode>> process(
            @RequestHeader("Idempotency-Key") String key,
            @RequestBody JsonNode payload) {

        JsonNode previous = completed.get(key);
        if (previous != null) {
            return Mono.just(ResponseEntity.ok(previous));
        }

        String mode = payload.path("simulate").asText("ok");

        if ("bad".equals(mode)) {
            return Mono.just(error(HttpStatus.UNPROCESSABLE_ENTITY, "payload rejected"));
        }
        if ("flaky".equals(mode)) {
            int call = flakyCalls.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            if (call <= 2) {
                return Mono.just(error(HttpStatus.SERVICE_UNAVAILABLE, "simulated outage, call " + call));
            }
            return Mono.just(success(key, payload));
        }
        if ("slow".equals(mode)) {
            return Mono.delay(Duration.ofSeconds(30)).map(tick -> success(key, payload));
        }
        return Mono.delay(Duration.ofMillis(200)).map(tick -> success(key, payload));
    }

    private ResponseEntity<JsonNode> success(String key, JsonNode payload) {
        ObjectNode out = mapper.createObjectNode();
        out.put("externalRef", "EXT-" + UUID.randomUUID());
        out.set("echo", payload);
        completed.put(key, out);
        JsonNode body = out;
        return ResponseEntity.ok(body);
    }

    private ResponseEntity<JsonNode> error(HttpStatus status, String message) {
        ObjectNode out = mapper.createObjectNode();
        out.put("error", message);
        JsonNode body = out;
        return ResponseEntity.status(status).body(body);
    }
}
