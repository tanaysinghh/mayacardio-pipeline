package com.mayacardio.pipeline.device.collector;

import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.RangeSet;
import com.mayacardio.pipeline.core.SampleBlock;
import com.mayacardio.pipeline.device.DeviceState;
import com.mayacardio.pipeline.device.store.DeviceStore;
import com.mayacardio.pipeline.mock.Generator;
import com.mayacardio.pipeline.mock.MockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CollectorFaultTest {
    private static final double SPEED = 5.0;

    @TempDir
    Path dir;
    private MockServer server;
    private final List<Runner> runners = new ArrayList<>();

    final class Runner {
        final DeviceStore store;
        final DeviceState state = new DeviceState();
        final Collector collector;
        final Thread thread;

        Runner(DiskGuard.SpaceProbe probe, int memCap) throws Exception {
            store = DeviceStore.open(dir.resolve("pipeline.db"));
            DiskGuard guard = new DiskGuard(dir, 1024 * 1024, 16L * 1024 * 1024, 48L * 1024 * 1024, probe);
            collector = new Collector(store, state, new MockClient("127.0.0.1", server.port(), 400), guard, 100, memCap);
            thread = new Thread(collector, "collector-test");
            thread.start();
            runners.add(this);
        }

        void stop() throws InterruptedException {
            collector.stop();
            thread.join(10_000);
            store.close();
            runners.remove(this);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (Runner r : new ArrayList<>(runners)) {
            r.stop();
        }
        if (server != null) {
            server.close();
        }
    }

    private void startMock() throws Exception {
        server = new MockServer(new Generator("ramp", 7), SPEED, 0, 10, 0, -1, null);
        server.start();
    }

    private Runner start() throws Exception {
        return new Runner(DiskGuard.REAL, 48_000);
    }

    private static void sleep(long ms) throws InterruptedException {
        Thread.sleep(ms);
    }

    record Verdict(long stored, Map<String, Long> lostByReason, long next1, long next2, List<LossRange> losses) {
    }

    private Verdict verify(String sessionId, Generator g) throws Exception {
        try (DeviceStore store = DeviceStore.open(dir.resolve("pipeline.db"))) {
            DeviceStore.SessionInfo info = store.session(sessionId);
            assertNotNull(info, "session " + sessionId + " not in db");
            List<LossRange> losses = store.losses(info.sid());
            Map<String, Long> byReason = new TreeMap<>();
            long stored = 0;
            long[] nexts = new long[3];
            for (int s = 1; s <= 2; s++) {
                DeviceStore.StreamState st = store.stream(info.sid(), s);
                nexts[s] = st.nextIndex();
                RangeSet lost = new RangeSet();
                long lostSum = 0;
                for (LossRange l : losses) {
                    if (l.sensor() != s) {
                        continue;
                    }
                    long end = l.toIndex() == null ? st.nextIndex() - 1 : l.toIndex();
                    if (l.toIndex() == null) {
                        assertEquals(st.nextIndex(), l.fromIndex(), "open-ended loss must start at next index");
                        byReason.merge(l.reason() + "(open)", 1L, Long::sum);
                        continue;
                    }
                    lost.add(l.fromIndex(), end);
                    lostSum += end - l.fromIndex() + 1;
                    byReason.merge(l.reason(), end - l.fromIndex() + 1, Long::sum);
                }
                assertEquals(lostSum, lost.count(), "overlapping losses on sensor " + s);
                SampleBlock b = store.block(info.sid(), s, 0, Math.max(0, st.nextIndex() - 1));
                long count = 0;
                RangeSet present = new RangeSet();
                for (long i = 0; i < st.nextIndex(); i++) {
                    boolean has = b.has(i);
                    if (has) {
                        present.add(i, i);
                    }
                    boolean isLost = lost.contains(i);
                    assertTrue(has ^ isLost, "sensor " + s + " index " + i + " stored=" + has + " lost=" + isLost);
                    if (has) {
                        count++;
                        assertTrue(Arrays.equals(g.sample(s, i), b.row(i)), "value mismatch at " + s + "/" + i);
                    }
                }
                assertEquals(count, st.storedCount(), "stored_count mismatch");
                assertEquals(present.toString(), store.storedRuns(info.sid(), s).toString(), "stored runs mismatch on sensor " + s);
                assertEquals(count, store.countSamples(info.sid(), s));
                stored += count;
            }
            return new Verdict(stored, byReason, nexts[1], nexts[2], losses);
        }
    }

    @Test
    void cleanRunLosesNothing() throws Exception {
        startMock();
        Runner r = start();
        sleep(3000);
        String sid = r.state.sessionId();
        r.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        assertTrue(v.lostByReason().isEmpty(), v.lostByReason().toString());
        assertTrue(v.next1() > 400 * SPEED * 2, "collected " + v.next1());
        long head = server.session().head();
        assertTrue(head - v.next1() < 400 * SPEED, "lag " + (head - v.next1()));
    }

    @Test
    void caughtUpCollectorDoesNotReReadEveryCycle() throws Exception {
        DeviceStore.open(dir.resolve("warm-up.db")).close();
        server = new MockServer(new Generator("ramp", 7), 1.0, 0, 10, 0, -1, null);
        server.start();
        Runner r = start();
        sleep(4000);
        long replies = r.state.repliesOk.get();
        String sid = r.state.sessionId();
        r.stop();
        assertTrue(replies <= 4000 / 100 * 2 * 13 / 10, "replies " + replies);
        assertTrue(verify(sid, new Generator("ramp", 7)).lostByReason().isEmpty());
    }

    @Test
    void corruptRepliesAreDiscardedAndReRead() throws Exception {
        startMock();
        Runner r = start();
        sleep(800);
        server.command("corrupt 7");
        sleep(1500);
        String sid = r.state.sessionId();
        long corrupt = r.state.corruptReplies.get();
        r.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        assertTrue(v.lostByReason().isEmpty(), v.lostByReason().toString());
        assertEquals(7, corrupt);
    }

    @Test
    void duplicateRepliesDoNotDuplicateOrShiftData() throws Exception {
        startMock();
        Runner r = start();
        sleep(800);
        server.command("duplicate 9");
        sleep(1500);
        String sid = r.state.sessionId();
        r.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        assertTrue(v.lostByReason().isEmpty(), v.lostByReason().toString());
    }

    @Test
    void shortStallWithinBufferLosesNothing() throws Exception {
        startMock();
        Runner r = start();
        sleep(800);
        server.command("stall 1.2");
        sleep(3000);
        String sid = r.state.sessionId();
        long timeouts = r.state.timeouts.get();
        r.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        assertTrue(v.lostByReason().isEmpty(), v.lostByReason().toString());
        assertTrue(timeouts >= 1);
    }

    @Test
    void longStallBeyondBufferIsRecordedAsBufferOverflow() throws Exception {
        startMock();
        Runner r = start();
        sleep(800);
        server.command("stall 3");
        sleep(4500);
        String sid = r.state.sessionId();
        r.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        assertTrue(v.lostByReason().containsKey("buffer_overflow"), v.lostByReason().toString());
        long lost = v.lostByReason().get("buffer_overflow");
        long perSensorExpected = (long) (400 * SPEED * 3) - 4000;
        assertTrue(lost > 2 * perSensorExpected - 1000 && lost < 2 * perSensorExpected + 6000, "lost " + lost);
    }

    @Test
    void unansweredSensorDoesNotStarveTheOther() throws Exception {
        startMock();
        Runner r = start();
        sleep(800);
        server.command("mute 2 4");
        sleep(6000);
        String sid = r.state.sessionId();
        r.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        for (LossRange l : v.losses()) {
            assertEquals(2, l.sensor(), "sensor 1 must not lose data: " + l);
            assertEquals("buffer_overflow", l.reason());
        }
        assertTrue(v.lostByReason().containsKey("buffer_overflow"), v.lostByReason().toString());
    }

    @Test
    void sensorOutageIsRecordedExactly() throws Exception {
        startMock();
        Runner r = start();
        sleep(800);
        server.command("outage 2 1.5");
        sleep(2500);
        String sid = r.state.sessionId();
        r.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        List<long[]> outages = server.session().outages(2);
        assertEquals(1, outages.size());
        long expected = outages.get(0)[1] - outages.get(0)[0] + 1;
        assertEquals(Map.of("sensor_outage", expected), v.lostByReason());
        RangeSet lost2 = new RangeSet();
        for (LossRange l : v.losses()) {
            assertEquals(2, l.sensor());
            lost2.add(l.fromIndex(), l.toIndex());
        }
        assertEquals("[" + outages.get(0)[0] + "-" + outages.get(0)[1] + "]", lost2.toString());
    }

    @Test
    void sessionRestartClosesOldSessionWithOpenEndedTailLoss() throws Exception {
        startMock();
        Runner r = start();
        sleep(1000);
        String first = r.state.sessionId();
        server.command("restart");
        sleep(2000);
        String second = r.state.sessionId();
        r.stop();
        assertTrue(!first.equals(second));
        Verdict old = verify(first, new Generator("ramp", 7));
        assertEquals(2L, old.lostByReason().get(Collector.R_SESSION_RESTART + "(open)"));
        Verdict fresh = verify(second, new Generator("ramp", 7));
        assertTrue(fresh.lostByReason().isEmpty(), fresh.lostByReason().toString());
        assertTrue(fresh.next1() > 0);
    }

    @Test
    void quickRestartRecoversFromMockBuffer() throws Exception {
        startMock();
        Runner r = start();
        sleep(1000);
        String sid = r.state.sessionId();
        r.stop();
        sleep(1200);
        Runner r2 = start();
        sleep(1500);
        assertEquals(sid, r2.state.sessionId());
        r2.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        assertTrue(v.lostByReason().isEmpty(), v.lostByReason().toString());
    }

    @Test
    void longDowntimeIsRecordedAsBufferOverflow() throws Exception {
        startMock();
        Runner r = start();
        sleep(800);
        String sid = r.state.sessionId();
        r.stop();
        sleep(3000);
        Runner r2 = start();
        sleep(1500);
        r2.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        assertTrue(v.lostByReason().containsKey("buffer_overflow"), v.lostByReason().toString());
        assertEquals(1, v.lostByReason().size());
    }

    @Test
    void diskFullBuffersInMemoryAndRecoversWithoutLoss() throws Exception {
        startMock();
        AtomicLong free = new AtomicLong(Long.MAX_VALUE);
        Runner r = new Runner(d -> free.get(), 48_000);
        sleep(800);
        free.set(1024);
        sleep(1500);
        assertTrue(r.state.diskFull());
        assertTrue(r.state.pendingSamples.get() > 0);
        free.set(Long.MAX_VALUE);
        sleep(1500);
        assertTrue(!r.state.diskFull());
        String sid = r.state.sessionId();
        r.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        assertTrue(v.lostByReason().isEmpty(), v.lostByReason().toString());
    }

    @Test
    void diskFullBeyondMemoryCapRecordsDiskFullLosses() throws Exception {
        startMock();
        AtomicLong free = new AtomicLong(Long.MAX_VALUE);
        Runner r = new Runner(d -> free.get(), 2000);
        sleep(800);
        free.set(1024);
        sleep(2000);
        free.set(Long.MAX_VALUE);
        sleep(1500);
        String sid = r.state.sessionId();
        r.stop();
        Verdict v = verify(sid, new Generator("ramp", 7));
        assertEquals(1, v.lostByReason().size(), v.lostByReason().toString());
        assertTrue(v.lostByReason().get(Collector.R_DISK_FULL) > 0);
    }

    @Test
    void coverSplitsByKnownReasonsAndFlagsUnreportedGaps() {
        List<LossRange> known = List.of(new LossRange(1, 100, 199L, "buffer_overflow"), new LossRange(1, 300, 349L, "sensor_outage"));
        List<LossRange> out = Collector.cover(1, 50, 399, known);
        assertEquals(List.of(
                new LossRange(1, 50, 99L, Collector.R_UNDELIVERED),
                new LossRange(1, 100, 199L, "buffer_overflow"),
                new LossRange(1, 200, 299L, Collector.R_UNDELIVERED),
                new LossRange(1, 300, 349L, "sensor_outage"),
                new LossRange(1, 350, 399L, Collector.R_UNDELIVERED)), out);
        assertNull(null);
    }
}
