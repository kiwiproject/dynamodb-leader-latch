package org.kiwiproject.dynamodb.leader;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Controllable {@link LockGateway} for unit tests.
 */
class FakeLockGateway implements LockGateway {

    final AtomicBoolean lockAvailable = new AtomicBoolean(true);
    final AtomicBoolean failAcquisition = new AtomicBoolean();
    final AtomicBoolean leaseHeld = new AtomicBoolean(true);
    final AtomicInteger releaseCount = new AtomicInteger();
    final AtomicInteger acquireAttempts = new AtomicInteger();
    final AtomicBoolean closed = new AtomicBoolean();
    volatile Runnable onLeaseInDanger;
    volatile String owner;

    @Override
    public Optional<Lease> tryAcquire(Runnable onLeaseInDanger) throws Exception {
        acquireAttempts.incrementAndGet();
        this.onLeaseInDanger = onLeaseInDanger;

        if (failAcquisition.get()) {
            throw new IllegalStateException("simulated DynamoDB error");
        }

        if (!lockAvailable.get()) {
            return Optional.empty();
        }

        leaseHeld.set(true);
        return Optional.of(new Lease() {
            @Override
            public boolean isHeld() {
                return leaseHeld.get();
            }

            @Override
            public void release() {
                releaseCount.incrementAndGet();
            }
        });
    }

    @Override
    public Optional<String> currentOwner() {
        return Optional.ofNullable(owner);
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
