package org.kiwiproject.dynamodb.leader;

/**
 * Receives notifications when a {@link LeaderLatch} gains or loses leadership.
 * <p>
 * Notifications are delivered only on actual state transitions, and never on the DynamoDB lock
 * client's heartbeat thread. Exceptions thrown by implementations are caught and logged; they do
 * not affect leader election.
 */
public interface LeaderLatchListener {

    /**
     * Called when this latch has become the leader.
     */
    void isLeader();

    /**
     * Called when this latch is no longer the leader, including when leadership can no longer
     * be guaranteed.
     */
    void notLeader();
}
