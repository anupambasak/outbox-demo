package com.example.outbox.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Fixed-delay background loop built from Reactor operators:
 * run the task, wait for it to finish, sleep, run it again.
 *
 * Runs never overlap, an exception in one run is logged and the loop carries on, and disposing the
 * returned handle stops it.
 */
public final class PollingLoop {

    private static final Logger log = LoggerFactory.getLogger(PollingLoop.class);

    private PollingLoop() {
    }

    public static Disposable start(String name, Duration delayBetweenRuns, Supplier<Mono<Void>> task) {
        return Mono.defer(task)
                .onErrorResume(e -> {
                    log.error("{} run failed, will retry after {}", name, delayBetweenRuns, e);
                    return Mono.empty();
                })
                .then(Mono.delay(delayBetweenRuns))
                .repeat()
                .subscribe(
                        ignored -> { },
                        e -> log.error("{} loop terminated unexpectedly", name, e));
    }
}
