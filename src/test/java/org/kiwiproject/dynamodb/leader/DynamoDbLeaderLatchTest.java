package org.kiwiproject.dynamodb.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dynamodb.leader.LeaderInfo.LookupFailed;
import org.kiwiproject.dynamodb.leader.LeadershipStatus.Closed;
import org.kiwiproject.dynamodb.leader.LeadershipStatus.IsLeader;
import org.kiwiproject.dynamodb.leader.LeadershipStatus.NotLeader;
import org.kiwiproject.dynamodb.leader.LeadershipStatus.NotStarted;
import org.kiwiproject.dynamodb.leader.LeadershipStatus.Uncertain;
import org.kiwiproject.dynamodb.leader.WhenLeaderResult.ActionFailed;
import org.kiwiproject.dynamodb.leader.WhenLeaderResult.RanAsLeader;
import org.kiwiproject.dynamodb.leader.WhenLeaderResult.SkippedNotLeader;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@DisplayName("DynamoDbLeaderLatch (state machine)")
class DynamoDbLeaderLatchTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    private FakeLockGateway gateway;
    private DynamoDbLeaderLatch latch;
    private List<String> events;

    @BeforeEach
    void setUp() {
        gateway = new FakeLockGateway();
        var config = LeaderLatchConfiguration.forTable("test-table")
                .withTimings(Duration.ofMillis(900), Duration.ofMillis(100))
                .withAcquisitionRetryInterval(Duration.ofMillis(50));
        latch = new DynamoDbLeaderLatch(config, "customer-service", "customer-service/1.0/host:8080", () -> gateway);
        events = new CopyOnWriteArrayList<>();
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                events.add("isLeader");
            }

            @Override
            public void notLeader() {
                events.add("notLeader");
            }
        });
    }

    @AfterEach
    void tearDown() {
        latch.close();
    }

    @Test
    void shouldHaveIdAndKey() {
        assertAll(
                () -> assertThat(latch.getId()).isEqualTo("customer-service/1.0/host:8080"),
                () -> assertThat(latch.getLeadershipKey()).isEqualTo("customer-service"),
                () -> assertThat(DynamoDbLeaderLatch.leaderLatchId("svc", "1.0", "host", 8080))
                        .isEqualTo("svc/1.0/host:8080")
        );
    }

    @Test
    void shouldIncludeIdKeyAndStateInToString() {
        assertThat(latch).hasToString(
                "DynamoDbLeaderLatch(id=customer-service/1.0/host:8080, leadershipKey=customer-service, state=NEW)");
    }

    @Nested
    class Validation {

        private final LeaderLatchConfiguration config = LeaderLatchConfiguration.forTable("t");

        @Test
        void shouldRejectBlankLeadershipKeyAndParticipantId() {
            assertAll(
                    () -> assertThatIllegalArgumentException()
                            .isThrownBy(() -> new DynamoDbLeaderLatch(config, " ", "id", () -> gateway))
                            .withMessage("leadershipKey must not be blank"),
                    () -> assertThatIllegalArgumentException()
                            .isThrownBy(() -> new DynamoDbLeaderLatch(config, "key", "", () -> gateway))
                            .withMessage("participantId must not be blank")
            );
        }

        @Test
        void shouldRejectNullArguments() {
            assertAll(
                    () -> assertThatIllegalArgumentException()
                            .isThrownBy(() -> new DynamoDbLeaderLatch(null, "key", "id", () -> gateway))
                            .withMessage("configuration must not be null"),
                    () -> assertThatIllegalArgumentException()
                            .isThrownBy(() -> new DynamoDbLeaderLatch(config, "key", "id", null))
                            .withMessage("gatewayFactory must not be null"),
                    () -> assertThatIllegalArgumentException()
                            .isThrownBy(() -> new DynamoDbLeaderLatch((DynamoDbClient) null, config, "key", "id"))
                            .withMessage("dynamoDbClient must not be null"),
                    () -> assertThatIllegalArgumentException()
                            .isThrownBy(() -> latch.addListener(null))
                            .withMessage("listener must not be null")
            );
        }
    }

    @Nested
    class Starting {

        @Test
        void shouldBeNotStartedBeforeStart() {
            assertAll(
                    () -> assertThat(latch.hasLeadership()).isFalse(),
                    () -> assertThat(latch.doesNotHaveLeadership()).isTrue(),
                    () -> assertThat(latch.checkLeadershipStatus()).isInstanceOf(NotStarted.class)
            );
        }

        @Test
        void shouldReturnStartedThenAlreadyStarted() {
            assertThat(latch.start()).isInstanceOf(StartResult.Started.class);
            assertThat(latch.start()).isInstanceOf(StartResult.AlreadyStarted.class);
        }

        @Test
        void shouldReturnFailedWhenGatewayCannotBeCreated() {
            var failure = new IllegalStateException("boom");
            var failingLatch = new DynamoDbLeaderLatch(
                    LeaderLatchConfiguration.forTable("t"), "key", "id", () -> {
                throw failure;
            });

            var result = failingLatch.start();

            assertAll(
                    () -> assertThat(result).isEqualTo(new StartResult.Failed(failure)),
                    () -> assertThat(failingLatch.checkLeadershipStatus()).isInstanceOf(NotStarted.class)
            );
        }

        @Test
        void shouldNotBlockWaitingForLeadership() {
            gateway.lockAvailable.set(false);

            var result = latch.start();

            assertAll(
                    () -> assertThat(result).isInstanceOf(StartResult.Started.class),
                    () -> assertThat(latch.hasLeadership()).isFalse()
            );
        }
    }

    @Nested
    class Leadership {

        @Test
        void shouldBecomeLeaderAndNotifyOnce() {
            latch.start();

            await().atMost(WAIT).until(latch::hasLeadership);
            assertThat(latch.checkLeadershipStatus()).isInstanceOf(IsLeader.class);

            // several more acquisition ticks must not produce more notifications
            var attempts = gateway.acquireAttempts.get();
            await().pollDelay(Duration.ofMillis(300)).atMost(WAIT).until(() -> true);
            assertAll(
                    () -> assertThat(gateway.acquireAttempts.get()).isEqualTo(attempts),
                    () -> assertThat(events).containsExactly("isLeader")
            );
        }

        @Test
        void shouldStayFollowerWhileLockHeldByAnother() {
            gateway.lockAvailable.set(false);
            latch.start();

            await().atMost(WAIT).until(() -> gateway.acquireAttempts.get() >= 3);
            assertAll(
                    () -> assertThat(latch.hasLeadership()).isFalse(),
                    () -> assertThat(latch.checkLeadershipStatus()).isInstanceOf(NotLeader.class),
                    () -> assertThat(events).isEmpty()
            );
        }

        @Test
        void shouldLoseLeadershipWhenLeaseCannotBeProven() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            gateway.lockAvailable.set(false);
            gateway.leaseHeld.set(false);

            await().atMost(WAIT).until(() -> events.contains("notLeader"));
            assertAll(
                    () -> assertThat(latch.hasLeadership()).isFalse(),
                    () -> assertThat(gateway.releaseCount).hasValue(1),
                    () -> assertThat(events).containsExactly("isLeader", "notLeader")
            );
        }

        @Test
        void shouldReportFalseImmediatelyWhenLeaseNotHeldEvenBeforeTickRuns() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            gateway.leaseHeld.set(false);

            assertThat(latch.hasLeadership()).isFalse();
        }

        @Test
        void shouldReacquireLeadershipAfterLosingIt() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            gateway.leaseHeld.set(false);
            await().atMost(WAIT).until(() -> events.contains("notLeader"));

            // lock is available again, so the next acquisition succeeds (the fake resets leaseHeld)
            await().atMost(WAIT).until(latch::hasLeadership);
            assertThat(events).containsExactly("isLeader", "notLeader", "isLeader");
        }

        @Test
        void shouldReactToLeaseInDangerCallbackFromLockClientThread() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            gateway.lockAvailable.set(false);
            gateway.leaseHeld.set(false);
            gateway.onLeaseInDanger.run();

            await().atMost(WAIT).until(() -> events.contains("notLeader"));
        }

        @Test
        void shouldNotReportDoesNotHaveLeadershipWhenLeader() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            assertThat(latch.doesNotHaveLeadership()).isFalse();
        }

        @Test
        void shouldReportNotLeaderWhenLeaseIsLostBeforeTheNextLeaseCheck() {
            // lease checks run every 15 seconds, so none happens during this test
            var slowConfig = LeaderLatchConfiguration.forTable("test-table")
                    .withTimings(Duration.ofSeconds(90), Duration.ofSeconds(30))
                    .withAcquisitionRetryInterval(Duration.ofMillis(50));

            try (var slowLatch = new DynamoDbLeaderLatch(
                    slowConfig, "customer-service", "customer-service/1.0/host:8080", () -> gateway)) {

                slowLatch.start();
                await().atMost(WAIT).until(slowLatch::hasLeadership);

                gateway.leaseHeld.set(false);

                assertAll(
                        () -> assertThat(slowLatch.hasLeadership()).isFalse(),
                        () -> assertThat(slowLatch.checkLeadershipStatus()).isInstanceOf(NotLeader.class)
                );
            }
        }

        @Test
        void shouldKeepTryingWhenAcquisitionIsInterrupted() {
            gateway.interruptAcquisition.set(true);
            latch.start();

            await().atMost(WAIT).until(() -> gateway.acquireAttempts.get() >= 3);
            assertThat(latch.hasLeadership()).isFalse();

            gateway.interruptAcquisition.set(false);
            await().atMost(WAIT).until(latch::hasLeadership);
        }

        @Test
        void shouldKeepCheckingTheLeaseWhenACheckThrowsUnexpectedly() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            gateway.failIsHeld.set(true);
            var callsBefore = gateway.isHeldCalls.get();
            await().atMost(WAIT).until(() -> gateway.isHeldCalls.get() >= callsBefore + 3);

            gateway.failIsHeld.set(false);
            gateway.lockAvailable.set(false);
            gateway.leaseHeld.set(false);

            await().atMost(WAIT).until(() -> events.contains("notLeader"));
        }

        @Test
        void shouldBeUncertainWhenAcquisitionFailsWithApiError() {
            gateway.failAcquisition.set(true);
            latch.start();

            await().atMost(WAIT).until(() -> latch.checkLeadershipStatus() instanceof Uncertain);
            assertThat(latch.hasLeadership()).isFalse();

            gateway.failAcquisition.set(false);
            await().atMost(WAIT).until(latch::hasLeadership);
        }
    }

    @Nested
    class Listeners {

        @Test
        void shouldNotLetListenerExceptionsBreakElectionOrOtherListeners() {
            latch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    throw new IllegalStateException("listener failure");
                }

                @Override
                public void notLeader() {
                    throw new IllegalStateException("listener failure");
                }
            });

            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            gateway.lockAvailable.set(false);
            gateway.leaseHeld.set(false);
            await().atMost(WAIT).until(() -> events.contains("notLeader"));
            assertThat(events).containsExactly("isLeader", "notLeader");
        }
    }

    @Nested
    class Closing {

        @Test
        void shouldReleaseLockAndNotifyWhenLeaderCloses() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            latch.close();

            assertAll(
                    () -> assertThat(latch.hasLeadership()).isFalse(),
                    () -> assertThat(latch.checkLeadershipStatus()).isInstanceOf(Closed.class),
                    () -> assertThat(gateway.releaseCount).hasValue(1),
                    () -> assertThat(gateway.closed).isTrue(),
                    () -> assertThat(events).containsExactly("isLeader", "notLeader")
            );
        }

        @Test
        void shouldDeliverNotLeaderWhenCloseIsCalledFromAListener() {
            latch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    latch.close();
                }

                @Override
                public void notLeader() {
                    // nothing to do
                }
            });

            latch.start();

            // would take the full close timeout, and drop the notification, if close() waited on its own thread
            await().atMost(Duration.ofSeconds(2)).until(() -> events.contains("notLeader"));
            assertAll(
                    () -> assertThat(events).containsExactly("isLeader", "notLeader"),
                    () -> assertThat(latch.checkLeadershipStatus()).isInstanceOf(Closed.class),
                    () -> assertThat(gateway.closed).isTrue()
            );
        }

        @Test
        void shouldBoundCloseTimeWhenReleaseHangs() {
            var gate = new CountDownLatch(1);
            gateway.releaseGate = gate;
            latch.setCloseTimeoutMillis(300);
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            try {
                var start = System.nanoTime();
                latch.close();
                var elapsedMillis = Duration.ofNanos(System.nanoTime() - start).toMillis();

                assertAll(
                        () -> assertThat(elapsedMillis).isLessThan(2_000),
                        () -> assertThat(latch.checkLeadershipStatus()).isInstanceOf(Closed.class),
                        () -> assertThat(events).containsExactly("isLeader", "notLeader")
                );
            } finally {
                gate.countDown();
            }
        }

        @Test
        void shouldIgnoreLeaseDangerCallbackAfterClose() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);
            var callback = gateway.onLeaseInDanger;

            latch.close();

            assertThatCode(callback::run).doesNotThrowAnyException();
        }

        @Test
        void shouldForceShutdownWhenAListenerOutlastsTheCloseTimeout() {
            var release = new CountDownLatch(1);
            latch.addListener(listenerBlockingOnNotLeader(release));
            latch.setCloseTimeoutMillis(300);
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            try {
                var start = System.nanoTime();
                latch.close();
                var elapsedMillis = Duration.ofNanos(System.nanoTime() - start).toMillis();

                assertAll(
                        () -> assertThat(elapsedMillis).isLessThan(2_000),
                        () -> assertThat(latch.checkLeadershipStatus()).isInstanceOf(Closed.class),
                        () -> assertThat(gateway.closed).isTrue()
                );
            } finally {
                release.countDown();
            }
        }

        @Test
        void shouldRestoreInterruptWhenTheClosingThreadIsInterrupted() throws InterruptedException {
            var release = new CountDownLatch(1);
            latch.addListener(listenerBlockingOnNotLeader(release));
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            var interruptRestored = new AtomicBoolean();
            var closer = new Thread(() -> {
                latch.close();
                interruptRestored.set(Thread.currentThread().isInterrupted());
            });
            closer.start();
            await().atMost(WAIT).until(() -> closer.getState() == Thread.State.TIMED_WAITING);

            try {
                closer.interrupt();
                closer.join(WAIT.toMillis());

                assertAll(
                        () -> assertThat(closer.isAlive()).isFalse(),
                        () -> assertThat(interruptRestored).isTrue(),
                        () -> assertThat(latch.checkLeadershipStatus()).isInstanceOf(Closed.class)
                );
            } finally {
                release.countDown();
            }
        }

        @Test
        void shouldBeIdempotent() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            latch.close();
            latch.close();
            latch.close();

            assertAll(
                    () -> assertThat(gateway.releaseCount).hasValue(1),
                    () -> assertThat(events).containsExactly("isLeader", "notLeader")
            );
        }

        @Test
        void shouldNotAcquireAfterClose() {
            gateway.lockAvailable.set(false);
            latch.start();
            await().atMost(WAIT).until(() -> gateway.acquireAttempts.get() >= 1);

            latch.close();
            gateway.lockAvailable.set(true);
            var attempts = gateway.acquireAttempts.get();

            await().pollDelay(Duration.ofMillis(300)).atMost(WAIT).until(() -> true);
            assertAll(
                    () -> assertThat(gateway.acquireAttempts).hasValue(attempts),
                    () -> assertThat(latch.hasLeadership()).isFalse()
            );
        }

        @Test
        void shouldNotRestartAfterClose() {
            latch.start();
            latch.close();

            assertThat(latch.start()).isInstanceOf(StartResult.Closed.class);
        }

        @Test
        void shouldCloseWithoutEverStarting() {
            latch.close();

            assertAll(
                    () -> assertThat(latch.checkLeadershipStatus()).isInstanceOf(Closed.class),
                    () -> assertThat(latch.start()).isInstanceOf(StartResult.Closed.class)
            );
        }
    }

    @Nested
    class GettingLeader {

        @Test
        void shouldFailLookupWhenNotStarted() {
            assertThat(latch.getLeader()).isInstanceOf(LookupFailed.class);
        }

        @Test
        void shouldReturnNoLeaderWhenThereIsNoRecord() {
            gateway.lockAvailable.set(false);
            latch.start();

            assertThat(latch.getLeader()).isInstanceOf(LeaderInfo.NoLeader.class);
        }

        @Test
        void shouldReturnLookupFailedWhenTheLookupThrows() {
            gateway.lockAvailable.set(false);
            gateway.failOwnerLookup.set(true);
            latch.start();

            assertThat(latch.getLeader())
                    .isInstanceOfSatisfying(LookupFailed.class,
                            failed -> assertThat(failed.cause()).hasMessage("simulated lookup error"));
        }

        @Test
        void shouldReturnRecordedOwner() {
            gateway.lockAvailable.set(false);
            gateway.owner = "other/1.0/host:9090";
            latch.start();

            assertThat(latch.getLeader())
                    .isInstanceOfSatisfying(LeaderInfo.Leader.class,
                            leader -> assertThat(leader.participantId()).isEqualTo("other/1.0/host:9090"));
        }
    }

    @Nested
    class WhenLeader {

        @Test
        void shouldSkipWhenNotLeader() {
            var result = latch.whenLeader(() -> "value");

            assertThat(result).isEqualTo(new SkippedNotLeader<String>(new NotStarted()));
        }

        @Test
        void shouldRunWhenLeader() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            var valueResult = latch.whenLeader(() -> "value");
            var runnableResult = latch.whenLeader(() -> events.add("ran"));

            assertAll(
                    () -> assertThat(valueResult).isEqualTo(new RanAsLeader<>("value")),
                    () -> assertThat(runnableResult).isInstanceOf(RanAsLeader.class),
                    () -> assertThat(events).contains("ran")
            );
        }

        @Test
        void shouldCaptureActionExceptions() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            var failure = new IllegalStateException("action failed");
            var result = latch.<String>whenLeader(() -> {
                throw failure;
            });

            assertThat(result).isEqualTo(new ActionFailed<String>(failure));
        }

        @Test
        void shouldSkipRunnableWhenNotLeader() {
            var ran = new AtomicBoolean();
            Runnable action = () -> ran.set(true);

            var result = latch.whenLeader(action);

            assertAll(
                    () -> assertThat(result).isEqualTo(new SkippedNotLeader<Void>(new NotStarted())),
                    () -> assertThat(ran).isFalse()
            );
        }

        @Test
        void shouldRunRunnableWhenLeader() {
            var ran = new AtomicBoolean();
            Runnable action = () -> ran.set(true);
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            var result = latch.whenLeader(action);

            assertAll(
                    () -> assertThat(result).isEqualTo(new RanAsLeader<Void>(null)),
                    () -> assertThat(ran).isTrue()
            );
        }

        @Test
        void shouldRunRunnableAsyncWhenLeader() {
            var ran = new AtomicBoolean();
            Runnable action = () -> ran.set(true);
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            var result = latch.whenLeaderAsync(action).orTimeout(5, TimeUnit.SECONDS).join();

            assertAll(
                    () -> assertThat(result).isEqualTo(new RanAsLeader<Void>(null)),
                    () -> assertThat(ran).isTrue()
            );
        }

        @Test
        void shouldRunAsyncOnTheGivenExecutor() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            var result = latch.whenLeaderAsync(() -> 42, Runnable::run).join();

            assertThat(result).isEqualTo(new RanAsLeader<>(42));
        }

        @Test
        void shouldRunAsyncWhenLeader() {
            latch.start();
            await().atMost(WAIT).until(latch::hasLeadership);

            var future = latch.whenLeaderAsync(() -> 42);

            assertThat(future.orTimeout(5, TimeUnit.SECONDS).join())
                    .isEqualTo(new RanAsLeader<>(42));
        }
    }

    private static LeaderLatchListener listenerBlockingOnNotLeader(CountDownLatch release) {
        return new LeaderLatchListener() {
            @Override
            public void isLeader() {
                // nothing to do
            }

            @Override
            public void notLeader() {
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
    }
}
