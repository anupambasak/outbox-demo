package com.example.outbox.domain;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.annotation.Indexed;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;

/**
 * The request record. It is simultaneously
 *  - the business data (payload / response),
 *  - the outbox entry (status RECEIVED means "not yet published"),
 *  - the dedup marker (COMPLETED / FAILED means "never process again").
 *
 * Because a single-record write in Aerospike is atomic, saving this one record is the
 * "business write + outbox write in one transaction" that the outbox pattern needs.
 *
 * {@code version} maps to the Aerospike record generation. Saving a loaded entity is a
 * compare-and-set: it fails with OptimisticLockingFailureException if anyone else wrote the record
 * in between. That is what keeps two publishers / two consumers from claiming the same record.
 */
@Document(collection = "api_requests")
public class ApiRequest {

    private static final int MAX_ERROR_CHARS = 500;

    @Id
    private String id;

    @Version
    private long version;

    @Indexed(type = IndexType.STRING, name = "api_requests_status_idx")
    private String status;

    private String payload;
    private String response;
    private String error;

    private int attempts;
    private long createdAt;
    private long updatedAt;

    /** Epoch millis until which {@link #claimedBy} owns the record; 0 = nobody. */
    private long claimUntil;
    private String claimedBy;

    /** Required by Spring Data for instantiation. */
    public ApiRequest() {
    }

    public static ApiRequest received(String id, String payload, long now) {
        ApiRequest r = new ApiRequest();
        r.id = id;
        r.status = Status.RECEIVED;
        r.payload = payload;
        r.createdAt = now;
        r.updatedAt = now;
        return r;
    }

    // ---- state queries --------------------------------------------------------------------

    public boolean isTerminal() {
        return Status.COMPLETED.equals(status) || Status.FAILED.equals(status);
    }

    /** True if another worker holds a live processing lease on this record. */
    public boolean isLeasedByOther(String me, long now) {
        return Status.PROCESSING.equals(status) && claimUntil > now && !me.equals(claimedBy);
    }

    // ---- state transitions (each one only mutates; the caller saves) ------------------------

    /** Publisher: take a short lease so other publisher instances skip this record. */
    public void claimForPublish(String owner, long now, long leaseMillis) {
        this.claimedBy = owner;
        this.claimUntil = now + leaseMillis;
        this.updatedAt = now;
    }

    /** Publisher: Kafka acknowledged the message. */
    public void markPublished(long now) {
        this.status = Status.PUBLISHED;
        clearClaim();
        this.updatedAt = now;
    }

    /** Consumer: start working on it. */
    public void startProcessing(String owner, long now, long leaseMillis) {
        this.status = Status.PROCESSING;
        this.claimedBy = owner;
        this.claimUntil = now + leaseMillis;
        this.attempts++;
        this.updatedAt = now;
    }

    /** Consumer: external API succeeded. */
    public void complete(String response, long now) {
        this.status = Status.COMPLETED;
        this.response = response;
        this.error = null;
        clearClaim();
        this.updatedAt = now;
    }

    /** Terminal failure: permanent API error, retries exhausted, or sweeper gave up. */
    public void fail(String reason, long now) {
        this.status = Status.FAILED;
        this.error = truncate(reason);
        clearClaim();
        this.updatedAt = now;
    }

    /** Consumer: transient error; hand the record back so the next Kafka retry can claim it. */
    public void releaseForRetry(String reason, long now) {
        this.status = Status.PUBLISHED;
        this.error = truncate(reason);
        clearClaim();
        this.updatedAt = now;
    }

    /** Sweeper: make the publisher send it again. */
    public void requeue(long now) {
        this.status = Status.RECEIVED;
        clearClaim();
        this.updatedAt = now;
    }

    private void clearClaim() {
        this.claimedBy = null;
        this.claimUntil = 0;
    }

    private static String truncate(String s) {
        if (s == null || s.length() <= MAX_ERROR_CHARS) {
            return s;
        }
        return s.substring(0, MAX_ERROR_CHARS) + "...";
    }

    // ---- getters --------------------------------------------------------------------------

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public String getStatus() {
        return status;
    }

    public String getPayload() {
        return payload;
    }

    public String getResponse() {
        return response;
    }

    public String getError() {
        return error;
    }

    public int getAttempts() {
        return attempts;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public long getClaimUntil() {
        return claimUntil;
    }

    public String getClaimedBy() {
        return claimedBy;
    }
}
