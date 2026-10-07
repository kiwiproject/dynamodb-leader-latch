package org.kiwiproject.dynamodb.leader;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.kiwiproject.dynamodb.leader.LockGateway.Lease;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A {@link LeaderLatch} that uses Amazon DynamoDB, via the AWS Labs amazon-dynamodb-lock-client.
 * <p>
 * All participants with the same leadership key contend for one lock record. The lock client's
 * background thread heartbeats the lease while this latch is the leader. If the heartbeat cannot be
 * maintained, this latch stops reporting leadership <em>before</em> the lease can be taken over,
 * and resumes trying to acquire it.
 * <p>
 * The table must already exist; the library never creates it. The caller owns the
 * {@link DynamoDbClient} and is responsible for closing it; this class never closes it.
 */
@Slf4j
public class DynamoDbLeaderLatch implements LeaderLatch {

    private enum State { NEW, STARTED, CLOSED }

    private static final long DEFAULT_CLOSE_TIMEOUT_MILLIS = 5_000;

    private final String id;
    private final String leadershipKey;
    private final LeaderLatchConfiguration configuration;
    private final Supplier<LockGateway> gatewayFactory;
    private final List<LeaderLatchListener> listeners = new CopyOnWriteArrayList<>();
    private final Object stateLock = new Object();

    private volatile State state = State.NEW;
    private volatile @Nullable LockGateway gateway;
    private volatile @Nullable ScheduledExecutorService executor;
    private volatile @Nullable Lease lease;
    private volatile @Nullable Throwable lastAcquisitionError;

    // only accessed on the latch executor thread
    private boolean acquisitionErrorLogged;

    private long closeTimeoutMillis = DEFAULT_CLOSE_TIMEOUT_MILLIS;

    /**
     * Create a latch.
     *
     * @param dynamoDbClient the AWS SDK v2 client to use; not closed by this latch
     * @param configuration  the latch configuration
     * @param leadershipKey  the key shared by all participants of the same election, e.g. "order-service"
     * @param participantId  the unique ID of this participant, e.g. from {@link #leaderLatchId}
     * @throws IllegalArgumentException if any argument is null or blank
     */
    public DynamoDbLeaderLatch(DynamoDbClient dynamoDbClient,
                               LeaderLatchConfiguration configuration,
                               String leadershipKey,
                               String participantId) {
        this(configuration, leadershipKey, participantId,
                gatewaySupplier(dynamoDbClient, configuration, leadershipKey, participantId));
    }

    @VisibleForTesting
    DynamoDbLeaderLatch(LeaderLatchConfiguration configuration,
                        String leadershipKey,
                        String participantId,
                        Supplier<LockGateway> gatewayFactory) {
        this.configuration = requireNotNull(configuration, "configuration must not be null");
        this.leadershipKey = requireNotBlank(leadershipKey, "leadershipKey must not be blank");
        this.id = requireNotBlank(participantId, "participantId must not be blank");
        this.gatewayFactory = requireNotNull(gatewayFactory, "gatewayFactory must not be null");
    }

    private static Supplier<LockGateway> gatewaySupplier(DynamoDbClient dynamoDbClient,
                                                         LeaderLatchConfiguration configuration,
                                                         String leadershipKey,
                                                         String participantId) {
        requireNotNull(dynamoDbClient, "dynamoDbClient must not be null");
        requireNotNull(configuration, "configuration must not be null");
        requireNotBlank(leadershipKey, "leadershipKey must not be blank");
        requireNotBlank(participantId, "participantId must not be blank");
        return () -> new AwsLockGateway(dynamoDbClient, configuration, leadershipKey, participantId);
    }

    /**
     * Generate a standard participant ID, in the same format used by dropwizard-leader-latch.
     *
     * @param serviceName    the name of the service
     * @param serviceVersion the version of the service
     * @param hostname       the host name where the service instance is running
     * @param port           the port on which the service instance is running
     * @return a participant ID
     */
    public static String leaderLatchId(String serviceName,
                                       String serviceVersion,
                                       String hostname,
                                       int port) {
        return f("{}/{}/{}:{}", serviceName, serviceVersion, hostname, port);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getLeadershipKey() {
        return leadershipKey;
    }

    @VisibleForTesting
    void setCloseTimeoutMillis(long closeTimeoutMillis) {
        this.closeTimeoutMillis = closeTimeoutMillis;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("leadershipKey", leadershipKey)
                .add("state", state)
                .toString();
    }

    @Override
    public StartResult start() {
        synchronized (stateLock) {
            if (state == State.CLOSED) {
                return new StartResult.Closed();
            }
            if (state == State.STARTED) {
                LOG.trace("start() already called for leader latch {} (key {}); ignoring", id, leadershipKey);
                return new StartResult.AlreadyStarted();
            }

            LockGateway newGateway = null;
            ScheduledExecutorService newExecutor = null;
            try {
                LOG.info("Starting leader latch {} for key {}", id, leadershipKey);
                newGateway = gatewayFactory.get();
                newExecutor = newExecutor();

                var retryMillis = configuration.acquisitionRetryInterval().toMillis();
                var watchMillis = Math.max(1, configuration.heartbeatPeriod().toMillis() / 2);

                gateway = newGateway;
                executor = newExecutor;
                state = State.STARTED;

                newExecutor.scheduleWithFixedDelay(() -> runSafely("acquisition", this::acquisitionTick),
                        0, retryMillis, TimeUnit.MILLISECONDS);
                newExecutor.scheduleWithFixedDelay(() -> runSafely("lease check", this::leaseWatchTick),
                        watchMillis, watchMillis, TimeUnit.MILLISECONDS);

                return new StartResult.Started();
            } catch (Exception e) {
                LOG.error("Unable to start leader latch {} for key {}", id, leadershipKey, e);
                state = State.NEW;
                gateway = null;
                executor = null;
                if (newExecutor != null) {
                    newExecutor.shutdownNow();
                }
                if (newGateway != null) {
                    newGateway.close();
                }
                return new StartResult.Failed(e);
            }
        }
    }

    private ScheduledExecutorService newExecutor() {
        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            var thread = new Thread(runnable, "dynamodb-leader-latch-" + leadershipKey);
            thread.setDaemon(true);
            return thread;
        });
    }

    private void runSafely(String taskName, Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            // never let an exception cancel the periodic task
            LOG.error("Unexpected error in {} for leader latch {} (key {})", taskName, id, leadershipKey, e);
        }
    }

    private void acquisitionTick() {
        var currentGateway = gateway;
        if (state != State.STARTED || currentGateway == null || lease != null) {
            return;
        }

        try {
            var acquired = currentGateway.tryAcquire(this::onLeaseInDanger);
            lastAcquisitionError = null;
            acquisitionErrorLogged = false;

            if (acquired.isPresent()) {
                becomeLeader(acquired.get());
            } else {
                LOG.trace("Leadership for key {} is held by another participant", leadershipKey);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            lastAcquisitionError = e;
            if (acquisitionErrorLogged) {
                LOG.debug("Error trying to acquire leadership for key {}", leadershipKey, e);
            } else {
                acquisitionErrorLogged = true;
                LOG.warn("Error trying to acquire leadership for key {}. Will keep retrying.", leadershipKey, e);
            }
        }
    }

    private void leaseWatchTick() {
        var current = lease;
        if (current != null && !current.isHeld()) {
            becomeFollower("lease can no longer be proven");
        }
    }

    // Called on a lock client heartbeat thread. Only hand off to the latch executor.
    private void onLeaseInDanger() {
        var currentExecutor = executor;
        if (currentExecutor == null) {
            return;
        }
        try {
            currentExecutor.execute(() -> runSafely("lease check", this::leaseWatchTick));
        } catch (RejectedExecutionException e) {
            LOG.trace("Latch executor is shut down; ignoring lease danger notification", e);
        }
    }

    private void becomeLeader(Lease newLease) {
        synchronized (stateLock) {
            if (state != State.STARTED || lease != null) {
                releaseQuietly(newLease);
                return;
            }
            lease = newLease;
        }

        LOG.info("Leadership acquired for key {} by {}", leadershipKey, id);
        notifyListeners(true);
    }

    private void becomeFollower(String reason) {
        Lease old;
        synchronized (stateLock) {
            old = lease;
            lease = null;
        }

        if (old == null) {
            return;
        }

        // Always release, otherwise the lock client could keep heartbeating a lease we have given up
        releaseQuietly(old);
        LOG.warn("Leadership lost for key {} by {}: {}", leadershipKey, id, reason);
        notifyListeners(false);
    }

    private void releaseQuietly(Lease toRelease) {
        try {
            toRelease.release();
        } catch (Exception e) {
            LOG.warn("Unable to release lock for key {}; lease expiry will allow failover", leadershipKey, e);
        }
    }

    private void notifyListeners(boolean leader) {
        for (var listener : listeners) {
            try {
                if (leader) {
                    listener.isLeader();
                } else {
                    listener.notLeader();
                }
            } catch (Exception e) {
                LOG.error("Leader latch listener {} threw an exception for key {}", listener, leadershipKey, e);
            }
        }
    }

    @Override
    public boolean hasLeadership() {
        var current = lease;
        return state == State.STARTED && current != null && current.isHeld();
    }

    @Override
    public LeadershipStatus checkLeadershipStatus() {
        switch (state) {
            case NEW:
                return new LeadershipStatus.NotStarted();
            case CLOSED:
                return new LeadershipStatus.Closed();
            default:
                break;
        }

        var current = lease;
        if (current != null) {
            return current.isHeld() ? new LeadershipStatus.IsLeader() : new LeadershipStatus.NotLeader();
        }

        var error = lastAcquisitionError;
        return error != null ? new LeadershipStatus.Uncertain(error) : new LeadershipStatus.NotLeader();
    }

    @Override
    public LeaderInfo getLeader() {
        var currentGateway = gateway;
        if (state != State.STARTED || currentGateway == null) {
            return new LeaderInfo.LookupFailed(new IllegalStateException("leader latch is not started"));
        }

        try {
            return currentGateway.currentOwner()
                    .<LeaderInfo>map(owner -> new LeaderInfo.Leader(owner, Instant.now()))
                    .orElseGet(LeaderInfo.NoLeader::new);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new LeaderInfo.LookupFailed(e);
        } catch (Exception e) {
            return new LeaderInfo.LookupFailed(e);
        }
    }

    @Override
    public void addListener(LeaderLatchListener listener) {
        listeners.add(requireNotNull(listener, "listener must not be null"));
    }

    @Override
    public void close() {
        Lease held;
        ScheduledExecutorService currentExecutor;
        LockGateway currentGateway;

        synchronized (stateLock) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSED;
            held = lease;
            lease = null;
            currentExecutor = executor;
            currentGateway = gateway;
        }

        LOG.info("Stopping leader latch {} for key {}", id, leadershipKey);

        if (currentExecutor != null) {
            stopExecutor(currentExecutor, held != null);
        }
        if (held != null) {
            LOG.info("Leadership lost for key {} by {}: latch closed", leadershipKey, id);
        }

        var gatewayToClose = currentGateway;
        runBounded("release lock and close lock client", () -> {
            if (held != null) {
                releaseQuietly(held);
            }
            if (gatewayToClose != null) {
                gatewayToClose.close();
            }
        });
    }

    private void stopExecutor(ScheduledExecutorService toStop, boolean notifyNotLeader) {
        try {
            if (notifyNotLeader) {
                // runs on the latch thread so it is ordered after any in-flight isLeader notification
                toStop.execute(() -> notifyListeners(false));
            }
        } catch (RejectedExecutionException e) {
            LOG.trace("Latch executor already shut down", e);
        }

        toStop.shutdown();
        try {
            if (!toStop.awaitTermination(closeTimeoutMillis, TimeUnit.MILLISECONDS)) {
                LOG.warn("Latch executor for key {} did not stop in {} ms; forcing", leadershipKey, closeTimeoutMillis);
                toStop.shutdownNow();
            }
        } catch (InterruptedException e) {
            toStop.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void runBounded(String description, Runnable task) {
        var thread = new Thread(task, "dynamodb-leader-latch-close-" + leadershipKey);
        thread.setDaemon(true);
        thread.start();
        try {
            thread.join(closeTimeoutMillis);
            if (thread.isAlive()) {
                LOG.warn("Timed out after {} ms trying to {} for key {}", closeTimeoutMillis, description, leadershipKey);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
