package com.example.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Typed view of everything under "app." in application.yml. */
@ConfigurationProperties(prefix = "app")
public record AppProperties(Topic topic,
                            Publisher publisher,
                            Processor processor,
                            Sweeper sweeper,
                            External external) {

    public record Topic(String name, int partitions, int replicas) {
    }

    public record Publisher(Duration pollInterval,
                            int batchSize,
                            int concurrency,
                            Duration claimLease,
                            Duration sendTimeout) {
    }

    public record Processor(Duration lease, int maxRetries) {
    }

    public record Sweeper(boolean enabled,
                          Duration interval,
                          Duration publishedStuckAfter,
                          int maxAttempts) {
    }

    public record External(String baseUrl, String path, Duration timeout, boolean mockEnabled) {
    }

    /**
     * Fail fast on settings that would silently break the lease logic: a lease shorter than the
     * work it protects lets a second worker take over a record that is still being handled.
     */
    public AppProperties {
        if (processor.lease().compareTo(external.timeout().multipliedBy(2)) <= 0) {
            throw new IllegalArgumentException(
                    "app.processor.lease (" + processor.lease() + ") must be more than twice app.external.timeout ("
                            + external.timeout() + ")");
        }
        if (publisher.claimLease().compareTo(publisher.sendTimeout().multipliedBy(2)) <= 0) {
            throw new IllegalArgumentException(
                    "app.publisher.claim-lease (" + publisher.claimLease() + ") must be more than twice app.publisher.send-timeout ("
                            + publisher.sendTimeout() + ")");
        }
        if (publisher.batchSize() < 1 || publisher.concurrency() < 1) {
            throw new IllegalArgumentException("app.publisher.batch-size and app.publisher.concurrency must be >= 1");
        }
    }
}
