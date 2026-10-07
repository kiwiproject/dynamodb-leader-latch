package org.kiwiproject.dynamodb.leader;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("StartResult")
class StartResultTest {

    @Test
    void shouldRejectNullCauseInFailed() {
        assertThatIllegalArgumentException().isThrownBy(() -> new StartResult.Failed(null));
    }
}
