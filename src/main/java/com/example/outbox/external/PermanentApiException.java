package com.example.outbox.external;

/** The external API rejected the request in a way that retrying will not fix (e.g. HTTP 400/404/422). */
public class PermanentApiException extends RuntimeException {

    public PermanentApiException(String message) {
        super(message);
    }
}
