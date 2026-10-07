package org.kiwiproject.dynamodb.leader;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

@DisplayName("LeaderInfo")
class LeaderInfoTest {

    @Test
    void shouldRejectInvalidLeaderArguments() {
        var now = Instant.now();

        assertAll(
                () -> assertThatIllegalArgumentException().isThrownBy(() -> new LeaderInfo.Leader(null, now)),
                () -> assertThatIllegalArgumentException().isThrownBy(() -> new LeaderInfo.Leader(" ", now)),
                () -> assertThatIllegalArgumentException().isThrownBy(() -> new LeaderInfo.Leader("id", null))
        );
    }

    @Test
    void shouldRejectNullCauseInLookupFailed() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LeaderInfo.LookupFailed(null));
    }
}
