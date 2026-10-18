package com.example.outbox.domain;

/**
 * Lifecycle of a request record.
 *
 * <pre>
 * RECEIVED --publisher--> PUBLISHED --consumer--> PROCESSING --+--> COMPLETED
 *     ^                       ^                       |        +--> FAILED
 *     |                       +---- transient error --+
 *     +------------- sweeper (stuck PUBLISHED / expired PROCESSING lease)
 * </pre>
 *
 * Plain String constants (not an enum) so the value stored in the Aerospike bin, the value used in
 * derived queries and the value in the secondary index are guaranteed to be the same literal.
 */
public final class Status {

    public static final String RECEIVED = "RECEIVED";
    public static final String PUBLISHED = "PUBLISHED";
    public static final String PROCESSING = "PROCESSING";
    public static final String COMPLETED = "COMPLETED";
    public static final String FAILED = "FAILED";

    private Status() {
    }
}
