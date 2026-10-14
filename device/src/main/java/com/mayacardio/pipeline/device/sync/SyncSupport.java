package com.mayacardio.pipeline.device.sync;

import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.device.DeviceState;
import com.mayacardio.pipeline.device.net.CloudClient;
import com.mayacardio.pipeline.device.store.DeviceStore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class SyncSupport {
    private static final int RESULTS_PER_POST = 500;

    private SyncSupport() {
    }

    static void syncLosses(DeviceStore store, CloudClient cloud, DeviceStore.SessionInfo s, long timeoutMs)
            throws SQLException, IOException {
        List<LossRange> unsynced = store.unsyncedLosses(s.sid());
        for (int i = 0; i < unsynced.size(); i += 1000) {
            List<LossRange> part = unsynced.subList(i, Math.min(unsynced.size(), i + 1000));
            cloud.postLosses(s.sessionId(), part, timeoutMs);
            store.markLossesSynced(s.sid(), new ArrayList<>(part));
        }
    }

    static void syncMeta(DeviceStore store, CloudClient cloud, DeviceStore.SessionInfo s, long timeoutMs)
            throws SQLException, IOException {
        if (!s.closed()) {
            return;
        }
        long[] sync = store.syncState(s.sid());
        if (sync[2] != 0) {
            return;
        }
        long n1 = store.stream(s.sid(), 1).nextIndex();
        long n2 = store.stream(s.sid(), 2).nextIndex();
        cloud.postMeta(s.sessionId(), true, n1, n2, s.closeReason(), timeoutMs);
        store.updateSync(s.sid(), null, null, true, null);
    }

    static void syncResults(DeviceStore store, CloudClient cloud, DeviceStore.SessionInfo s, DeviceState state, long timeoutMs)
            throws SQLException, IOException {
        while (true) {
            List<DeviceStore.StoredResult> rs = store.results(s.sid(), true, RESULTS_PER_POST);
            if (rs.isEmpty()) {
                return;
            }
            List<String> lines = new ArrayList<>();
            for (DeviceStore.StoredResult r : rs) {
                lines.add(r.body());
            }
            cloud.postResults(s.sessionId(), lines, timeoutMs);
            store.markResultsSynced(s.sid(), rs);
            if (rs.size() < RESULTS_PER_POST) {
                return;
            }
        }
    }
}
