package com.mayacardio.pipeline.device;

import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.device.store.DeviceStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VerifyCommandTest {

    private static int[][] rows(int n) {
        int[][] r = new int[n][];
        for (int k = 0; k < n; k++) {
            r[k] = new int[]{k, k, k};
        }
        return r;
    }

    @Test
    void healthyLedgerPassesAndOverlapFails(@TempDir Path dir) throws Exception {
        Properties p = new Properties();
        p.setProperty("data.dir", dir.toString());
        DeviceConfig cfg = DeviceConfig.of(p);
        long sid;
        try (DeviceStore store = DeviceStore.open(cfg.dbPath())) {
            sid = store.createSession("v1");
            store.commit(new DeviceStore.Commit(sid, 1, List.of(), 0, rows(100), 100, false));
            store.commit(new DeviceStore.Commit(sid, 1, List.of(new LossRange(1, 100, 149L, "sensor_outage")), 150, rows(50), 200, false));
            store.closeSession(sid, "test", List.of(new LossRange(1, 200, null, "tail"), new LossRange(2, 0, null, "tail")));
        }
        assertEquals(0, Main.verify(cfg));
        try (DeviceStore store = DeviceStore.open(cfg.dbPath())) {
            store.closeSession(sid, "test", List.of(new LossRange(1, 120, 160L, "overlap")));
        }
        assertEquals(1, Main.verify(cfg));
    }
}
