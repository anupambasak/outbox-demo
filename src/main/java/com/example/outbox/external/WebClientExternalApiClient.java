package com.example.outbox.external;

import com.example.outbox.config.AppProperties;
import io.netty.channel.ChannelOption;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

/**
 * Calls the external API with a non-blocking WebClient.
 *
 * ASSUMED CONTRACT (adjust here if the real API differs):
 *   POST {base-url}{path}
 *   Content-Type: application/json
 *   Idempotency-Key: &lt;record id&gt;
 *   body: the stored JSON payload
 *   2xx -> success, response body is stored on the record
 *   408 / 429 / 5xx -> transient (retried);  other 4xx -> permanent (record FAILED)
 */
@Component
public class WebClientExternalApiClient implements ExternalApiClient {

    private static final int MAX_ERROR_BODY_CHARS = 300;

    private final WebClient webClient;
    private final String path;
    private final Duration timeout;

    public WebClientExternalApiClient(WebClient.Builder builder, AppProperties props) {
        AppProperties.External cfg = props.external();

        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) Math.min(cfg.timeout().toMillis(), 5_000L))
                .responseTimeout(cfg.timeout());

        this.webClient = builder
                .baseUrl(cfg.baseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
        this.path = cfg.path();
        this.timeout = cfg.timeout();
    }

    @Override
    public Mono<String> call(String idempotencyKey, String jsonPayload) {
        return webClient.post()
                .uri(path)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", idempotencyKey)
                .bodyValue(jsonPayload)
                .retrieve()
                // Order matters: the first matching handler wins.
                .onStatus(status -> status.value() == 408 || status.value() == 429,
                        resp -> errorBody(resp).map(body ->
                                new TransientApiException("HTTP " + resp.statusCode().value() + ": " + body)))
                .onStatus(HttpStatusCode::is4xxClientError,
                        resp -> errorBody(resp).map(body ->
                                new PermanentApiException("HTTP " + resp.statusCode().value() + ": " + body)))
                .onStatus(HttpStatusCode::is5xxServerError,
                        resp -> errorBody(resp).map(body ->
                                new TransientApiException("HTTP " + resp.statusCode().value() + ": " + body)))
                .bodyToMono(String.class)
                .defaultIfEmpty("")
                .timeout(timeout)
                // Connection refused, reset, DNS failure, TimeoutException ... all worth retrying.
                .onErrorMap(
                        ex -> !(ex instanceof PermanentApiException) && !(ex instanceof TransientApiException),
                        ex -> new TransientApiException("External call failed: " + ex, ex));
    }

    private static Mono<String> errorBody(ClientResponse response) {
        return response.bodyToMono(String.class)
                .defaultIfEmpty("")
                .map(body -> body.length() <= MAX_ERROR_BODY_CHARS
                        ? body
                        : body.substring(0, MAX_ERROR_BODY_CHARS) + "...");
    }
}
