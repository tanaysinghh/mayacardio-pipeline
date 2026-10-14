package com.mayacardio.pipeline.cloud;

import com.mayacardio.pipeline.cloud.service.IngestService;
import com.mayacardio.pipeline.cloud.service.ProcessingService;
import com.mayacardio.pipeline.cloud.service.SessionRegistry;
import com.mayacardio.pipeline.cloud.service.SessionState;
import com.mayacardio.pipeline.cloud.store.CloudStore;
import com.mayacardio.pipeline.cloud.store.JdbcCloudStore;
import com.mayacardio.pipeline.cloud.store.Segment;
import com.mayacardio.pipeline.core.BatchCodec;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.MemorySignalView;
import com.mayacardio.pipeline.core.ResultJson;
import com.mayacardio.pipeline.core.SampleRun;
import com.mayacardio.pipeline.core.WindowProcessor;
import com.mayacardio.pipeline.core.WindowResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteDataSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CloudPipelineTest {

    @TempDir
    Path dir;

    private static int[] sample(int sensor, long i) {
        double t = i / 400.0;
        if (sensor == 2) {
            return new int[]{400000 + (int) (20000 * Math.sin(t / 5)), 150000 + (int) (i % 97)};
        }
        double frac = t - Math.floor(t);
        int ecg = (int) (1_000_000 * Math.exp(-0.5 * Math.pow((frac - 0.3) / 0.008, 2))) + (int) ((i * 7919) % 300);
        return new int[]{(int) Math.round(0.6 * ecg), ecg, (int) Math.round(1.3 * ecg)};
    }

    protected CloudStore newStore() {
        SQLiteDataSource ds = new SQLiteDataSource();
        ds.setUrl("jdbc:sqlite:" + dir.resolve("cloud.db"));
        JdbcCloudStore store = new JdbcCloudStore(ds, JdbcCloudStore.Dialect.SQLITE);
        store.init();
        return store;
    }

    private static List<SampleRun> runs(int sensor, long from, long to, java.util.function.LongPredicate keep) {
        List<SampleRun> out = new ArrayList<>();
        List<int[]> cur = new ArrayList<>();
        long first = -1;
        for (long i = from; i <= to; i++) {
            if (keep.test(i)) {
                if (cur.isEmpty()) {
                    first = i;
                }
                cur.add(sample(sensor, i));
            } else if (!cur.isEmpty()) {
                out.add(new SampleRun(sensor, first, cur.toArray(new int[0][])));
                cur = new ArrayList<>();
            }
        }
        if (!cur.isEmpty()) {
            out.add(new SampleRun(sensor, first, cur.toArray(new int[0][])));
        }
        return out;
    }

    private static void awaitIdle(SessionState st, ProcessingService p, int expected) throws InterruptedException {
        for (int k = 0; k < 200 && st.computedCount() < expected; k++) {
            Thread.sleep(50);
            p.signal(st);
        }
    }

    @Test
    void segmentRoundTrip() {
        Segment s = new Segment(12, 3);
        s.put(0, new int[]{-8388608, 0, 8388607});
        s.put(399, new int[]{1, 2, 3});
        s.put(200, new int[]{5, -5, 5});
        Segment d = Segment.decode(12, s.encode());
        assertEquals(3, d.count());
        assertArrayEquals(new int[]{-8388608, 0, 8388607}, d.row(0));
        assertArrayEquals(new int[]{1, 2, 3}, d.row(399));
        assertFalse(d.has(1));
        assertEquals(Segment.Put.SAME, d.put(399, new int[]{1, 2, 3}));
        assertEquals(Segment.Put.CONFLICT, d.put(399, new int[]{1, 2, 4}));
    }

    @Test
    void postgresUrlConversion() {
        String[] r = CloudConfig.toJdbc("postgresql://alice:p%40ss@ep-x.neon.tech/neondb?sslmode=require", "", "");
        assertEquals("jdbc:postgresql://ep-x.neon.tech/neondb?sslmode=require", r[0]);
        assertEquals("alice", r[1]);
        assertEquals("p@ss", r[2]);
        assertEquals("jdbc:postgresql://h:5432/db?sslmode=require", CloudConfig.toJdbc("postgres://h:5432/db", "u", "p")[0]);
    }

    @Test
    void cloudResultsEqualCoreResultsWithOutOfOrderDuplicateAndLostData() throws Exception {
        long n = 400 * 40;
        long lostFrom = 400 * 21 + 17;
        long lostTo = 400 * 22 + 50;
        java.util.function.LongPredicate keep1 = i -> i < lostFrom || i > lostTo;
        java.util.function.LongPredicate keep2 = i -> i % 1000 != 0;
        MemorySignalView ref = new MemorySignalView();
        for (long i = 0; i < n; i++) {
            if (keep1.test(i)) {
                ref.put(1, i, sample(1, i));
            }
            if (keep2.test(i)) {
                ref.put(2, i, sample(2, i));
            }
        }
        ref.setResolvedEnd(1, n);
        ref.setResolvedEnd(2, n);
        ref.setSessionEnd(1, n);
        ref.setSessionEnd(2, n);

        CloudStore store = newStore();
        SessionRegistry registry = new SessionRegistry(store);
        ProcessingService processing = new ProcessingService(registry);
        IngestService ingest = new IngestService(registry, processing);
        List<BatchCodec.Batch> batches = new ArrayList<>();
        for (long a = 0; a < n; a += 1000) {
            long b = Math.min(n - 1, a + 999);
            List<SampleRun> rr = new ArrayList<>(runs(1, a, b, keep1));
            rr.addAll(runs(2, a, b, keep2));
            batches.add(BatchCodec.decode(BatchCodec.encode(new BatchCodec.Batch("s1", 1, rr))));
        }
        List<BatchCodec.Batch> shuffled = new ArrayList<>(batches);
        Collections.shuffle(shuffled, new Random(3));
        shuffled.addAll(batches.subList(0, 5));
        int half = shuffled.size() / 2;
        for (BatchCodec.Batch b : shuffled.subList(0, half)) {
            ingest.ingest(b);
        }
        processing.close();

        SessionRegistry registry2 = new SessionRegistry(store);
        ProcessingService processing2 = new ProcessingService(registry2);
        IngestService ingest2 = new IngestService(registry2, processing2);
        for (BatchCodec.Batch b : shuffled.subList(half, shuffled.size())) {
            ingest2.ingest(b);
        }
        List<LossRange> losses = new ArrayList<>();
        losses.add(new LossRange(1, lostFrom, lostTo, "sensor_outage"));
        for (long i = 0; i < n; i += 1000) {
            losses.add(new LossRange(2, i, i, "buffer_overflow"));
        }
        ingest2.addLosses("s1", losses);
        ingest2.setMeta("s1", true, n, n, "session_end");
        SessionState st = registry2.get("s1");
        awaitIdle(st, processing2, 40);
        assertEquals(40, st.computedCount());
        assertEquals(n - (lostTo - lostFrom + 1), st.heldCount(1));

        Map<Long, WindowResult> cloud = new HashMap<>();
        store.forEachResult("s1", ResultJson.ONLINE, r -> cloud.put(r.window(), ResultJson.parse(Json.read(r.body()))));
        assertEquals(40, cloud.size());
        int withHr = 0;
        for (long w = 0; w < 40; w++) {
            WindowResult expected = WindowProcessor.compute(ref, w);
            assertEquals(expected, cloud.get(w), "window " + w);
            if (expected.heartRate() != null) {
                withHr++;
            }
        }
        assertTrue(withHr > 20);
        processing2.close();
    }

    @Test
    void windowWaitsForUnresolvedDataThenComputes() throws Exception {
        CloudStore store = newStore();
        SessionRegistry registry = new SessionRegistry(store);
        ProcessingService processing = new ProcessingService(registry);
        IngestService ingest = new IngestService(registry, processing);
        List<SampleRun> rr = new ArrayList<>(runs(1, 0, 400 * 10, i -> i < 1000 || i > 1200));
        rr.addAll(runs(2, 0, 400 * 10, i -> true));
        ingest.ingest(new BatchCodec.Batch("s2", 1, rr));
        SessionState st = registry.get("s2");
        awaitIdle(st, processing, 10);
        assertEquals(2, st.computedCount());
        ingest.addLosses("s2", List.of(new LossRange(1, 1000, 1200L, "x")));
        awaitIdle(st, processing, 10);
        assertEquals(10, st.computedCount());
        processing.close();
    }
}
