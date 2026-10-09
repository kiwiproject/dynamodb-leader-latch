package org.kiwiproject.dynamodb.leader;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

import java.time.Instant;

/**
 * The outcome of asking which participant is the leader, as recorded in DynamoDB.
 * <p>
 * This reflects the lock record, which can still name a crashed leader until its lease expires and
 * another participant takes over. Combine it with each participant's own
 * {@link LeaderLatch#hasLeadership()} to detect disagreement or more than one self-reported leader.
 */
public sealed interface LeaderInfo {

    /**
     * DynamoDB records the given participant as the leader.
     *
     * @param participantId the ID of the participant named in the lock record
     * @param observedAt    when the record was read
     */
    record Leader(String participantId, Instant observedAt) implements LeaderInfo {
        /**
         * Validates the participant ID and observation time.
         *
         * @throws IllegalArgumentException if the participant ID is blank or the time is null
         */
        public Leader {
            checkArgumentNotBlank(participantId, "participantId must not be blank");
            checkArgumentNotNull(observedAt, "observedAt must not be null");
        }
    }

    /**
     * There is no lock record, so no participant is currently recorded as the leader.
     */
    record NoLeader() implements LeaderInfo {}

    /**
     * The leader could not be determined.
     *
     * @param cause the error that occurred looking up the lock record
     */
    record LookupFailed(Throwable cause) implements LeaderInfo {
        /**
         * Validates the cause.
         *
         * @throws IllegalArgumentException if the cause is null
         */
        public LookupFailed {
            checkArgumentNotNull(cause, "cause must not be null");
        }
    }
}
