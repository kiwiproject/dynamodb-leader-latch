package org.kiwiproject.dynamodb.leader;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Elects a single leader among multiple participants (for example, instances of one logical service).
 * <p>
 * Expected distributed-systems failures are reported as values, not exceptions: methods on this
 * interface do not throw because DynamoDB is unreachable or a lock is held by someone else.
 * <p>
 * Leadership is a lease. If the latch cannot confidently prove it still owns the lease, it behaves
 * as a follower.
 */
public interface LeaderLatch extends AutoCloseable {

    /**
     * The ID of this participant, which is recorded as the owner of the lock when this latch is the leader.
     *
     * @return the participant ID
     */
    String getId();

    /**
     * The key identifying the election, e.g. a logical service name. All participants contending
     * for the same leadership use the same key.
     *
     * @return the leadership key
     */
    String getLeadershipKey();

    /**
     * Begin participating in the election and return immediately; this never waits to become the leader.
     * <p>
     * The returned {@link StartResult} says only whether participation began. Leadership, if won, is
     * acquired afterward on the latch's own thread, and can be gained and lost many times, which is why
     * this does not return a one-shot future. Observe leadership with {@link #hasLeadership()},
     * {@link #checkLeadershipStatus()}, or a {@link LeaderLatchListener}.
     *
     * @return whether participation in the election began
     */
    StartResult start();

    /**
     * Check whether this participant is currently the leader. Never throws.
     *
     * @return true only if the latch is started and can currently prove it holds the lease
     * @see #checkLeadershipStatus()
     */
    boolean hasLeadership();

    /**
     * Check leadership, returning a status that callers can switch over.
     *
     * @return the current status
     */
    LeadershipStatus checkLeadershipStatus();

    /**
     * Look up which participant DynamoDB currently records as leader, from any participant.
     *
     * @return the outcome
     * @see LeaderInfo
     */
    LeaderInfo getLeader();

    /**
     * Add a listener that is notified when leadership is acquired or lost.
     *
     * @param listener the listener
     */
    void addListener(LeaderLatchListener listener);

    /**
     * Stop participating, releasing the lock if held. Idempotent, bounded in time, and never throws.
     */
    @Override
    void close();

    /**
     * The negation of {@link #hasLeadership()}.
     *
     * @return true if this latch is not provably the leader
     */
    default boolean doesNotHaveLeadership() {
        return !hasLeadership();
    }

    /**
     * Run the action synchronously only if this latch is the leader.
     *
     * @param action the action
     * @return the outcome; never throws, even if the action does
     */
    default WhenLeaderResult<Void> whenLeader(Runnable action) {
        return whenLeader(() -> {
            action.run();
            return null;
        });
    }

    /**
     * Run the action synchronously only if this latch is the leader.
     *
     * @param action the action
     * @param <T>    the result type
     * @return the outcome; never throws, even if the action does
     */
    default <T> WhenLeaderResult<T> whenLeader(Supplier<T> action) {
        var status = checkLeadershipStatus();
        if (!status.isLeader()) {
            return new WhenLeaderResult.SkippedNotLeader<>(status);
        }

        try {
            return new WhenLeaderResult.RanAsLeader<>(action.get());
        } catch (Exception e) {
            return new WhenLeaderResult.ActionFailed<>(e);
        }
    }

    /**
     * Run the action asynchronously (on the common pool) only if this latch is the leader.
     *
     * @param action the action
     * @return a future that always completes normally with the outcome
     */
    default CompletableFuture<WhenLeaderResult<Void>> whenLeaderAsync(Runnable action) {
        return CompletableFuture.supplyAsync(() -> whenLeader(action));
    }

    /**
     * Run the action asynchronously (on the common pool) only if this latch is the leader.
     *
     * @param action the action
     * @param <T>    the result type
     * @return a future that always completes normally with the outcome
     */
    default <T> CompletableFuture<WhenLeaderResult<T>> whenLeaderAsync(Supplier<T> action) {
        return CompletableFuture.supplyAsync(() -> whenLeader(action));
    }

    /**
     * Run the action asynchronously on the given executor only if this latch is the leader.
     *
     * @param action   the action
     * @param executor the executor
     * @param <T>      the result type
     * @return a future that always completes normally with the outcome
     */
    default <T> CompletableFuture<WhenLeaderResult<T>> whenLeaderAsync(Supplier<T> action, Executor executor) {
        return CompletableFuture.supplyAsync(() -> whenLeader(action), executor);
    }
}
