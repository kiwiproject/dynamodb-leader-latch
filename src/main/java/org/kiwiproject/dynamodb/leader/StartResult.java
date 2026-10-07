package org.kiwiproject.dynamodb.leader;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

/**
 * The outcome of {@link LeaderLatch#start()}. Starting never blocks waiting for leadership.
 */
public sealed interface StartResult {

    /**
     * The latch started and is now participating in the election.
     */
    record Started() implements StartResult {}

    /**
     * The latch was already started; nothing was changed.
     */
    record AlreadyStarted() implements StartResult {}

    /**
     * The latch has been closed and cannot be started.
     */
    record Closed() implements StartResult {}

    /**
     * The latch could not be started. It may be started again.
     *
     * @param cause the reason it could not be started
     */
    record Failed(Throwable cause) implements StartResult {
        public Failed {
            checkArgumentNotNull(cause, "cause must not be null");
        }
    }
}
