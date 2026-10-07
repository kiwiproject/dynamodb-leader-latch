package org.kiwiproject.dynamodb.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("LeadershipStatus")
class LeadershipStatusTest {

    @Test
    void shouldRejectNullCauseInUncertain() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LeadershipStatus.Uncertain(null));
    }

    @Test
    void shouldOnlyTreatIsLeaderAsLeader() {
        assertAll(
                () -> assertThat(new LeadershipStatus.IsLeader().isLeader()).isTrue(),
                () -> assertThat(new LeadershipStatus.NotLeader().isLeader()).isFalse(),
                () -> assertThat(new LeadershipStatus.NotStarted().isLeader()).isFalse(),
                () -> assertThat(new LeadershipStatus.Closed().isLeader()).isFalse(),
                () -> assertThat(
                        new LeadershipStatus.Uncertain(new IllegalStateException("x")).isLeader()).isFalse()
        );
    }
}
