package org.kiwiproject.dynamodb.leader;

import java.util.Optional;

/**
 * Small package-private seam over the DynamoDB lock client so the latch's state machine can be
 * unit tested without mocking AWS classes.
 */
interface LockGateway extends AutoCloseable {

    /**
     * Try to acquire the lock without waiting for leadership.
     *
     * @param onLeaseInDanger invoked (possibly on a lock client thread) when the lease has gone too long
     *                        without a successful heartbeat; must not block
     * @return the lease if acquired, or empty if the lock is held by someone else
     * @throws Exception for any DynamoDB/API error
     */
    Optional<Lease> tryAcquire(Runnable onLeaseInDanger) throws Exception;

    /**
     * Read the participant ID that DynamoDB records as the lock owner.
     *
     * @return the owner, or empty if there is no lock record
     * @throws Exception for any DynamoDB/API error
     */
    Optional<String> currentOwner() throws Exception;

    @Override
    void close();

    /**
     * A held lease.
     */
    interface Lease {

        /**
         * Whether the lease can still be proven. Must not throw; any doubt means false.
         *
         * @return true only if the lease is definitely still held
         */
        boolean isHeld();

        /**
         * Release the lease.
         *
         * @throws Exception if the release fails
         */
        void release() throws Exception;
    }
}
