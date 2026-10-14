package com.mayacardio.pipeline.device.store;

import com.mayacardio.pipeline.core.SampleBlock;
import com.mayacardio.pipeline.core.SampleRun;
import com.mayacardio.pipeline.core.Signal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeviceStoreTest {

    private static int[] row(long i) {
        return new int[]{(int) (i * 7 % 1000), Signal.MIN_VALUE + (int) i, Signal.MAX_VALUE - (int) i};
    }

    private static int[][] rows(long from, int n) {
        int[][] r = new int[n][];
        for (int k = 0; k < n; k++) {
            r[k] = row(from + k);
        }
        return r;
    }

    @Test
    void codecRoundTripsExtremes() {
        int[][] r = {{Signal.MIN_VALUE, Signal.MAX_VALUE, 0}, {Signal.MAX_VALUE, Signal.MIN_VALUE, -1}, {5, 5, 5}};
        int[][] back = BlockCodec.decode(BlockCodec.encode(r, 3), 3, 3);
        for (int k = 0; k < r.length; k++) {
            assertArrayEquals(r[k], back[k]);
        }
    }

    @Test
    void blocksAreReadAcrossBoundariesAndOverlapIsTrimmed(@TempDir Path dir) throws Exception {
        try (DeviceStore store = DeviceStore.open(dir.resolve("p.db"))) {
            long sid = store.createSession("t");
            store.commit(new DeviceStore.Commit(sid, 1, List.of(), 0, rows(0, 300), 300, false));
            store.commit(new DeviceStore.Commit(sid, 1, List.of(), 250, rows(250, 200), 450, false));
            store.commit(new DeviceStore.Commit(sid, 1, List.of(), 600, rows(600, 100), 700, false));
            assertEquals(550, store.countSamples(sid, 1));
            assertEquals(550, store.stream(sid, 1).storedCount());
            SampleBlock b = store.block(sid, 1, 290, 610);
            for (long i = 290; i <= 610; i++) {
                assertEquals(i < 450 || i >= 600, b.has(i), "index " + i);
                if (b.has(i)) {
                    assertArrayEquals(row(i), b.row(i));
                }
            }
            List<SampleRun> runs = store.runs(sid, 1, 100, 650);
            assertEquals(2, runs.size());
            assertEquals(100, runs.get(0).firstIndex());
            assertEquals(449, runs.get(0).lastIndex());
            assertEquals(600, runs.get(1).firstIndex());
            assertEquals(650, runs.get(1).lastIndex());
            assertEquals("[0-449, 600-699]", store.storedRuns(sid, 1).toString());
            List<Long> seen = new ArrayList<>();
            store.forEachSample(sid, 1, (s, idx, v) -> {
                seen.add(idx);
                assertArrayEquals(row(idx), v);
            });
            assertEquals(550, seen.size());
            assertTrue(seen.get(0) == 0 && seen.get(549) == 699);
            assertFalse(store.block(sid, 2, 0, 10).has(0));
        }
    }
}
