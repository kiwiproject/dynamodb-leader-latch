package org.kiwiproject.dynamodb.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

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

        assertThat(config.tableName()).isEqualTo("leader-locks");
        assertThat(config.leaseDuration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.heartbeatPeriod()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.acquisitionRetryInterval()).isEqualTo(Duration.ofSeconds(5));
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

        assertThatIllegalArgumentException()
                .isThrownBy(() -> config.withTimings(Duration.ZERO, Duration.ofSeconds(1)));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> config.withTimings(Duration.ofSeconds(30), Duration.ofSeconds(-1)));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> config.withAcquisitionRetryInterval(Duration.ZERO));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> config.withAcquisitionRetryInterval(null));
    }

    @Test
    void shouldRequireSafetyMarginBetweenHeartbeatAndLease() {
        var config = LeaderLatchConfiguration.forTable("t");

        assertThatIllegalArgumentException()
                .isThrownBy(() -> config.withTimings(Duration.ofSeconds(10), Duration.ofSeconds(10)))
                .withMessageContaining("heartbeatPeriod");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> config.withTimings(Duration.ofSeconds(8), Duration.ofSeconds(3)));

        assertThat(config.withTimings(Duration.ofSeconds(9), Duration.ofSeconds(3)).leaseDuration())
                .isEqualTo(Duration.ofSeconds(9));
    }
}
