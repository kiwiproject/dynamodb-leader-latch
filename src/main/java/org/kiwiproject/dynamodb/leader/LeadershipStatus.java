package org.kiwiproject.dynamodb.leader;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

/**
 * The outcome of checking whether a {@link LeaderLatch} currently has leadership.
 * <p>
 * Only {@link IsLeader} means leader-only work may proceed. Every other status, including
 * {@link Uncertain}, must be treated as "not the leader".
 */
public sealed interface LeadershipStatus {

    /**
     * Check whether this is the one status in which leader-only work is allowed.
     *
     * @return true only for {@link IsLeader}
     */
    default boolean isLeader() {
        return this instanceof IsLeader;
    }

    /**
     * The latch holds the lease and can currently prove it.
     */
    record IsLeader() implements LeadershipStatus {}

    /**
     * The latch is running and is a follower.
     */
    record NotLeader() implements LeadershipStatus {}

    /**
     * The latch has not been started.
     */
    record NotStarted() implements LeadershipStatus {}

    /**
     * The latch has been closed.
     */
    record Closed() implements LeadershipStatus {}

    /**
     * Ownership could not be determined, for example because DynamoDB could not be reached.
     * The latch behaves as a follower until it can prove otherwise.
     *
     * @param cause the error that prevented determining the status
     */
    record Uncertain(Throwable cause) implements LeadershipStatus {
        public Uncertain {
            checkArgumentNotNull(cause, "cause must not be null");
        }
    }
}
