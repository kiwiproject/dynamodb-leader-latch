package org.kiwiproject.dynamodb.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kiwiproject.dynamodb.leader.LeaderInfo.Leader;
import org.kiwiproject.dynamodb.leader.LeaderInfo.NoLeader;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

@DisplayName("DynamoDbLeaderLatch (DynamoDB Local)")
@ExtendWith(DynamoDbLocalExtension.class)
class DynamoDbLeaderLatchIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(3);
    private static final Duration HEARTBEAT = Duration.ofSeconds(1);
    private static final Duration WAIT = Duration.ofSeconds(30);

    private final List<DynamoDbLeaderLatch> latches = new ArrayList<>();
    private final List<DynamoDbClient> clients = new ArrayList<>();
    private final String key = "service-" + UUID.randomUUID();

    @AfterEach
    void tearDown() {
        latches.forEach(DynamoDbLeaderLatch::close);
        clients.forEach(DynamoDbClient::close);
    }

    private DynamoDbLeaderLatch newLatch(String participantId) {
        return newLatch(participantId, DynamoDbLocalExtension.newClient());
    }

    private DynamoDbLeaderLatch newLatch(String participantId, DynamoDbClient client) {
        return newLatch(participantId, client, Duration.ofMillis(500));
    }

    private DynamoDbLeaderLatch newLatch(String participantId, DynamoDbClient client, Duration retryInterval) {
        clients.add(client);
        var config = LeaderLatchConfiguration.forTable(DynamoDbLocalExtension.TABLE_NAME)
                .withTimings(LEASE, HEARTBEAT)
                .withAcquisitionRetryInterval(retryInterval);
        var latch = new DynamoDbLeaderLatch(client, config, key, participantId);
        latches.add(latch);
        return latch;
    }

    private List<DynamoDbLeaderLatch> leaders() {
        return latches.stream().filter(DynamoDbLeaderLatch::hasLeadership).toList();
    }

    @Test
    void shouldElectExactlyOneLeaderAndNeverHaveTwo() {
        var a = newLatch("a");
        var b = newLatch("b");
        var c = newLatch("c");
        latches.forEach(l -> assertThat(l.start()).isInstanceOf(StartResult.Started.class));

        await().atMost(WAIT).until(() -> leaders().size() == 1);

        // sample for a meaningful interval (longer than a lease) and verify there is never more than one
        var sawMultiple = new AtomicBoolean();
        await().during(Duration.ofSeconds(5)).atMost(WAIT).until(() -> {
            if (leaders().size() > 1) {
                sawMultiple.set(true);
            }
            return !sawMultiple.get() && leaders().size() == 1;
        });

        var leader = leaders().get(0);
        assertThat(leader).isIn(a, b, c);
        assertThat(leader.checkLeadershipStatus()).isInstanceOf(LeadershipStatus.IsLeader.class);

        // every participant, leader or not, can see who the leader is
        latches.forEach(l -> assertThat(l.getLeader())
                .isInstanceOfSatisfying(Leader.class,
                        info -> assertThat(info.participantId()).isEqualTo(leader.getId())));
    }

    @Test
    void shouldFailOverAfterGracefulLeaderShutdown() {
        newLatch("a");
        newLatch("b");
        newLatch("c");
        latches.forEach(DynamoDbLeaderLatch::start);
        await().atMost(WAIT).until(() -> leaders().size() == 1);
        var firstLeader = leaders().get(0);

        firstLeader.close();

        await().atMost(WAIT).until(() -> leaders().size() == 1 && leaders().get(0) != firstLeader);
        var newLeader = leaders().get(0);
        assertThat(newLeader.getId()).isNotEqualTo(firstLeader.getId());
    }

    @Test
    void shouldFailOverAfterLeaderDisappearsWithoutReleasingLock() {
        var doomedClient = DynamoDbLocalExtension.newClient();
        var doomed = newLatch("doomed", doomedClient);
        doomed.start();
        await().atMost(WAIT).until(doomed::hasLeadership);

        var survivor = newLatch("survivor");
        survivor.start();
        assertThat(survivor.hasLeadership()).isFalse();

        // simulate the leader's process losing DynamoDB connectivity: heartbeats and release now fail
        doomedClient.close();

        await().atMost(WAIT).until(survivor::hasLeadership);
        assertThat(doomed.hasLeadership()).isFalse();
    }

    @Test
    void shouldReacquireLeadershipAfterLosingIt() {
        var events = new CopyOnWriteArrayList<String>();
        var a = newLatch("a");
        a.addListener(listenerRecordingTo(events));
        a.start();
        await().atMost(WAIT).until(a::hasLeadership);

        var b = newLatch("b");
        b.start();

        a.close();
        await().atMost(WAIT).until(b::hasLeadership);

        assertThat(events).containsExactly("isLeader", "notLeader");

        // a fresh latch for the same participant can lead again once b steps down
        b.close();
        var aAgain = newLatch("a");
        aAgain.start();
        await().atMost(WAIT).until(aAgain::hasLeadership);
    }

    @Test
    void shouldReportNewLeaderAfterFailover() {
        var a = newLatch("a");
        a.start();
        await().atMost(WAIT).until(a::hasLeadership);

        var observer = newLatch("observer");
        observer.start();
        assertThat(observer.getLeader())
                .isInstanceOfSatisfying(Leader.class, info -> assertThat(info.participantId()).isEqualTo("a"));

        a.close();
        await().atMost(WAIT).until(observer::hasLeadership);
        assertThat(observer.getLeader())
                .isInstanceOfSatisfying(Leader.class, info -> assertThat(info.participantId()).isEqualTo("observer"));
    }

    @Test
    void shouldReportNoLeaderWhenLockWasReleasedAndNobodyHasTakenOverYet() {
        var a = newLatch("a");
        a.start();
        await().atMost(WAIT).until(a::hasLeadership);

        // this follower will not try to acquire again for an hour, so nobody takes over after a closes
        var slowFollower = newLatch("slow-follower", DynamoDbLocalExtension.newClient(), Duration.ofHours(1));
        slowFollower.start();

        // the first acquisition attempt runs asynchronously; wait for it so it cannot run after a closes
        await().atMost(WAIT).until(() -> slowFollower.acquisitionAttemptCount() >= 1);
        assertThat(slowFollower.hasLeadership()).isFalse();

        a.close();

        assertThat(slowFollower.getLeader()).isInstanceOf(NoLeader.class);
    }

    private static LeaderLatchListener listenerRecordingTo(List<String> events) {
        return new LeaderLatchListener() {
            @Override
            public void isLeader() {
                events.add("isLeader");
            }

            @Override
            public void notLeader() {
                events.add("notLeader");
            }
        };
    }
}
