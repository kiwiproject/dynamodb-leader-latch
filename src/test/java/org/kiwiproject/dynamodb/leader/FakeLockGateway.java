package org.kiwiproject.dynamodb.leader;

import static java.util.Objects.nonNull;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Controllable {@link LockGateway} for unit tests.
 */
class FakeLockGateway implements LockGateway {

    final AtomicBoolean lockAvailable = new AtomicBoolean(true);
    final AtomicBoolean failAcquisition = new AtomicBoolean();
    final AtomicBoolean interruptAcquisition = new AtomicBoolean();
    final AtomicBoolean failOwnerLookup = new AtomicBoolean();
    final AtomicBoolean failIsHeld = new AtomicBoolean();
    final AtomicInteger isHeldCalls = new AtomicInteger();
    final AtomicBoolean leaseHeld = new AtomicBoolean(true);
    final AtomicInteger releaseCount = new AtomicInteger();
    final AtomicInteger acquireAttempts = new AtomicInteger();
    final AtomicBoolean closed = new AtomicBoolean();
    volatile CountDownLatch releaseGate;
    volatile Runnable onLeaseInDanger;
    volatile String owner;

    @Override
    public Optional<Lease> tryAcquire(Runnable onLeaseInDanger) throws InterruptedException {
        acquireAttempts.incrementAndGet();
        this.onLeaseInDanger = onLeaseInDanger;

        if (interruptAcquisition.get()) {
            throw new InterruptedException("simulated interrupt");
        }

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
                isHeldCalls.incrementAndGet();
                if (failIsHeld.get()) {
                    throw new IllegalStateException("simulated lease check error");
                }
                return leaseHeld.get();
            }

            @Override
            public void release() {
                var gate = releaseGate;
                if (nonNull(gate)) {
                    awaitGate(gate);
                }
                releaseCount.incrementAndGet();
            }
        });
    }

    private static void awaitGate(CountDownLatch gate) {
        try {
            gate.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Optional<String> currentOwner() {
        if (failOwnerLookup.get()) {
            throw new IllegalStateException("simulated lookup error");
        }
        return Optional.ofNullable(owner);
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
