package com.example.outbox.external;

/** The external API call failed in a way that may succeed on retry (5xx, 408, 429, timeout, I/O error). */
public class TransientApiException extends RuntimeException {

    public TransientApiException(String message) {
        super(message);
    }

    public TransientApiException(String message, Throwable cause) {
        super(message, cause);
    }
}
