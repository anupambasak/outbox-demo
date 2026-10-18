package com.example.outbox.domain;

import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import reactor.core.publisher.Flux;

public interface ApiRequestRepository extends ReactiveAerospikeRepository<ApiRequest, String> {

    /** Derived query; served by the secondary index on "status" declared on the entity. */
    Flux<ApiRequest> findByStatus(String status);
}
