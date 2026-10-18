package com.example.outbox.web;

import com.example.outbox.domain.ApiRequest;

/**
 * What the REST API returns for a request. payload and response are the raw JSON/text strings as
 * stored, so they appear as strings inside this JSON document.
 */
public record ApiRequestView(String id,
                             String status,
                             String payload,
                             String response,
                             String error,
                             int attempts,
                             long createdAt,
                             long updatedAt) {

    public static ApiRequestView from(ApiRequest r) {
        return new ApiRequestView(
                r.getId(),
                r.getStatus(),
                r.getPayload(),
                r.getResponse(),
                r.getError(),
                r.getAttempts(),
                r.getCreatedAt(),
                r.getUpdatedAt());
    }
}
