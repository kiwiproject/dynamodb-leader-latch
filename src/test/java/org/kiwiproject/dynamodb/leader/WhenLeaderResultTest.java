package org.kiwiproject.dynamodb.leader;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("WhenLeaderResult")
class WhenLeaderResultTest {

    @Test
    void shouldRejectNullArguments() {
        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> new WhenLeaderResult.SkippedNotLeader<String>(null)),
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> new WhenLeaderResult.ActionFailed<String>(null))
        );
    }

    @Test
    void shouldAllowNullValueWhenRanAsLeader() {
        assertThat(new WhenLeaderResult.RanAsLeader<Void>(null).value()).isNull();
    }
}
