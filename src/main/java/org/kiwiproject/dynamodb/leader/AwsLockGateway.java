package org.kiwiproject.dynamodb.leader;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.GetLockOptions;
import com.amazonaws.services.dynamodbv2.LockItem;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * {@link LockGateway} backed by the AWS Labs {@link AmazonDynamoDBLockClient}.
 * <p>
 * The lock client is configured with an automatic heartbeat thread and
 * {@code holdLockOnServiceUnavailable=false}, so a DynamoDB outage never extends a lease.
 */
@Slf4j
final class AwsLockGateway implements LockGateway {

    static final String PARTITION_KEY_NAME = "key";

    private final AmazonDynamoDBLockClient lockClient;
    private final String leadershipKey;
    private final long safeTimeWithoutHeartbeatMillis;

    AwsLockGateway(DynamoDbClient dynamoDbClient,
                   LeaderLatchConfiguration configuration,
                   String leadershipKey,
                   String participantId) {

        requireNotNull(dynamoDbClient, "dynamoDbClient must not be null");
        requireNotNull(configuration, "configuration must not be null");
        requireNotBlank(participantId, "participantId must not be blank");
        this.leadershipKey = requireNotBlank(leadershipKey, "leadershipKey must not be blank");

        var leaseMillis = configuration.leaseDuration().toMillis();
        var heartbeatMillis = configuration.heartbeatPeriod().toMillis();

        // Stop trusting the lease one heartbeat period before the lease duration elapses
        this.safeTimeWithoutHeartbeatMillis = leaseMillis - heartbeatMillis;

        var options = AmazonDynamoDBLockClientOptions.builder(dynamoDbClient, configuration.tableName())
                .withPartitionKeyName(PARTITION_KEY_NAME)
                .withOwnerName(participantId)
                .withLeaseDuration(leaseMillis)
                .withHeartbeatPeriod(heartbeatMillis)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withCreateHeartbeatBackgroundThread(true)
                .withHoldLockOnServiceUnavailable(false)
                .build();

        this.lockClient = new AmazonDynamoDBLockClient(options);
    }

    @Override
    public Optional<Lease> tryAcquire(Runnable onLeaseInDanger) throws InterruptedException {
        var acquireOptions = AcquireLockOptions.builder(leadershipKey)
                .withDeleteLockOnRelease(true)
                .withShouldSkipBlockingWait(true)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withSessionMonitor(safeTimeWithoutHeartbeatMillis, Optional.of(onLeaseInDanger))
                .build();

        return lockClient.tryAcquireLock(acquireOptions).map(AwsLease::new);
    }

    @Override
    public Optional<String> currentOwner() {
        var getOptions = GetLockOptions.builder(leadershipKey).build();
        return lockClient.getLockFromDynamoDB(getOptions).map(LockItem::getOwnerName);
    }

    @Override
    public void close() {
        try {
            lockClient.close();
        } catch (IOException e) {
            LOG.warn("Error closing DynamoDB lock client for key {}", leadershipKey, e);
        }
    }

    private final class AwsLease implements Lease {

        private final LockItem lockItem;

        private AwsLease(LockItem lockItem) {
            this.lockItem = lockItem;
        }

        @Override
        public boolean isHeld() {
            try {
                return !lockItem.isExpired() && !lockItem.amIAboutToExpire();
            } catch (RuntimeException e) {
                LOG.debug("Unable to prove lease for key {} is held; assuming not held", leadershipKey, e);
                return false;
            }
        }

        @Override
        public void release() {
            lockClient.releaseLock(lockItem);
        }
    }
}
