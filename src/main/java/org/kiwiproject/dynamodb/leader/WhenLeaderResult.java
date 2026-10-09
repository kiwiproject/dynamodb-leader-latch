package org.kiwiproject.dynamodb.leader;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

import org.jspecify.annotations.Nullable;

/**
 * The outcome of {@code whenLeader} and {@code whenLeaderAsync}.
 *
 * @param <T> the type of value produced by the action
 */
public sealed interface WhenLeaderResult<T> {

    /**
     * The latch was the leader and the action completed.
     *
     * @param value the action's result; null for a {@code Runnable} action or a null result
     * @param <T>   the result type
     */
    record RanAsLeader<T>(@Nullable T value) implements WhenLeaderResult<T> {}

    /**
     * The action was not run because the latch was not the leader.
     *
     * @param status the leadership status that prevented running the action
     * @param <T>    the result type
     */
    record SkippedNotLeader<T>(LeadershipStatus status) implements WhenLeaderResult<T> {
        /**
         * Validates the status.
         *
         * @throws IllegalArgumentException if the status is null
         */
        public SkippedNotLeader {
            checkArgumentNotNull(status, "status must not be null");
        }
    }

    /**
     * The latch was the leader, but the action threw an exception.
     *
     * @param error the exception thrown by the action
     * @param <T>   the result type
     */
    record ActionFailed<T>(Throwable error) implements WhenLeaderResult<T> {
        /**
         * Validates the error.
         *
         * @throws IllegalArgumentException if the error is null
         */
        public ActionFailed {
            checkArgumentNotNull(error, "error must not be null");
        }
    }
}
