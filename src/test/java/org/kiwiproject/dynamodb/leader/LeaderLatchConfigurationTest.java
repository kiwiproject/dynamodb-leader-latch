package org.kiwiproject.dynamodb.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

@DisplayName("LeaderLatchConfiguration")
class LeaderLatchConfigurationTest {

    @Test
    void shouldHaveDefaults() {
        var config = LeaderLatchConfiguration.forTable("leader-locks");

        assertAll(
                () -> assertThat(config.tableName()).isEqualTo("leader-locks"),
                () -> assertThat(config.leaseDuration()).isEqualTo(Duration.ofSeconds(30)),
                () -> assertThat(config.heartbeatPeriod()).isEqualTo(Duration.ofSeconds(5)),
                () -> assertThat(config.acquisitionRetryInterval()).isEqualTo(Duration.ofSeconds(5))
        );
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void shouldRejectBlankTableName(String tableName) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> LeaderLatchConfiguration.forTable(tableName));
    }

    @Test
    void shouldRejectNonPositiveDurations() {
        var config = LeaderLatchConfiguration.forTable("t");

        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withTimings(Duration.ZERO, Duration.ofSeconds(1)))
                        .withMessage("leaseDuration must be positive"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withTimings(Duration.ofSeconds(30), Duration.ofSeconds(-1)))
                        .withMessage("heartbeatPeriod must be positive"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withAcquisitionRetryInterval(Duration.ZERO))
                        .withMessage("acquisitionRetryInterval must be positive")
        );
    }

    @Test
    void shouldRejectNullDurations() {
        var config = LeaderLatchConfiguration.forTable("t");

        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withTimings(null, Duration.ofSeconds(1)))
                        .withMessage("leaseDuration must not be null"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withTimings(Duration.ofSeconds(30), null))
                        .withMessage("heartbeatPeriod must not be null"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withAcquisitionRetryInterval(null))
                        .withMessage("acquisitionRetryInterval must not be null")
        );
    }

    @Test
    void shouldRequireSafetyMarginBetweenHeartbeatAndLease() {
        var config = LeaderLatchConfiguration.forTable("t");

        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withTimings(Duration.ofSeconds(10), Duration.ofSeconds(10)))
                        .withMessage("leaseDuration (PT10S) must be at least 3 times heartbeatPeriod (PT10S)"),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> config.withTimings(Duration.ofSeconds(8), Duration.ofSeconds(3))),
                () -> assertThat(config.withTimings(Duration.ofSeconds(9), Duration.ofSeconds(3)).leaseDuration())
                        .isEqualTo(Duration.ofSeconds(9))
        );
    }
}
