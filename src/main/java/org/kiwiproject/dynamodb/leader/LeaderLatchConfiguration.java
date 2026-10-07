package org.kiwiproject.dynamodb.leader;

import static com.google.common.base.Preconditions.checkArgument;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.time.KiwiDurations.isPositive;

import java.time.Duration;

/**
 * Configuration for a {@link DynamoDbLeaderLatch}.
 * <p>
 * Leadership is a lease. If the leader cannot refresh the lease (for example, because DynamoDB is
 * unreachable) it stops considering itself the leader before another participant can take over.
 * To leave a safety margin, the lease duration must be at least {@value #MIN_LEASE_TO_HEARTBEAT_RATIO}
 * times the heartbeat period.
 *
 * @param tableName                the existing DynamoDB table; the partition key must be a string attribute named
 *                                 {@code key}. The library never creates the table.
 * @param leaseDuration            how long a lease is valid without being refreshed
 * @param heartbeatPeriod          how often the leader refreshes its lease
 * @param acquisitionRetryInterval how often a follower tries to acquire leadership
 */
public record LeaderLatchConfiguration(String tableName,
                                       Duration leaseDuration,
                                       Duration heartbeatPeriod,
                                       Duration acquisitionRetryInterval) {

    /**
     * The minimum ratio of lease duration to heartbeat period.
     */
    public static final int MIN_LEASE_TO_HEARTBEAT_RATIO = 3;

    /**
     * Default lease duration.
     */
    public static final Duration DEFAULT_LEASE_DURATION = Duration.ofSeconds(30);

    /**
     * Default heartbeat period.
     */
    public static final Duration DEFAULT_HEARTBEAT_PERIOD = Duration.ofSeconds(5);

    /**
     * Default acquisition retry interval.
     */
    public static final Duration DEFAULT_ACQUISITION_RETRY_INTERVAL = Duration.ofSeconds(5);

    /**
     * Validates the configuration.
     *
     * @throws IllegalArgumentException if any value is missing, non-positive, or the lease duration is
     *                                  less than {@value #MIN_LEASE_TO_HEARTBEAT_RATIO} times the heartbeat period
     */
    public LeaderLatchConfiguration {
        checkArgumentNotBlank(tableName, "tableName must not be blank");
        checkPositive(leaseDuration, "leaseDuration");
        checkPositive(heartbeatPeriod, "heartbeatPeriod");
        checkPositive(acquisitionRetryInterval, "acquisitionRetryInterval");
        checkArgument(leaseDuration.compareTo(heartbeatPeriod.multipliedBy(MIN_LEASE_TO_HEARTBEAT_RATIO)) >= 0,
                "leaseDuration (%s) must be at least %s times heartbeatPeriod (%s)",
                leaseDuration, MIN_LEASE_TO_HEARTBEAT_RATIO, heartbeatPeriod);
    }

    private static void checkPositive(Duration duration, String name) {
        checkArgumentNotNull(duration, "{} must not be null", name);
        checkArgument(isPositive(duration), "%s must be positive", name);
    }

    /**
     * Create a configuration with default timings.
     *
     * @param tableName the DynamoDB table name
     * @return a new configuration
     */
    public static LeaderLatchConfiguration forTable(String tableName) {
        return new LeaderLatchConfiguration(tableName,
                DEFAULT_LEASE_DURATION, DEFAULT_HEARTBEAT_PERIOD, DEFAULT_ACQUISITION_RETRY_INTERVAL);
    }

    /**
     * Create a copy with different lease and heartbeat timings.
     *
     * @param leaseDuration   the new lease duration
     * @param heartbeatPeriod the new heartbeat period
     * @return a new configuration
     */
    public LeaderLatchConfiguration withTimings(Duration leaseDuration, Duration heartbeatPeriod) {
        return new LeaderLatchConfiguration(tableName, leaseDuration, heartbeatPeriod, acquisitionRetryInterval);
    }

    /**
     * Create a copy with a different acquisition retry interval.
     *
     * @param acquisitionRetryInterval the new retry interval
     * @return a new configuration
     */
    public LeaderLatchConfiguration withAcquisitionRetryInterval(Duration acquisitionRetryInterval) {
        return new LeaderLatchConfiguration(tableName, leaseDuration, heartbeatPeriod, acquisitionRetryInterval);
    }
}
