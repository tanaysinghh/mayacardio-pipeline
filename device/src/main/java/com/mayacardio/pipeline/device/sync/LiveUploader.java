package com.mayacardio.pipeline.device.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.BatchCodec;
import com.mayacardio.pipeline.core.SampleRun;
import com.mayacardio.pipeline.device.Clocks;
import com.mayacardio.pipeline.device.DeviceState;
import com.mayacardio.pipeline.device.net.CloudClient;
import com.mayacardio.pipeline.device.net.ConnectivityMonitor;
import com.mayacardio.pipeline.device.store.DeviceStore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;

public final class LiveUploader implements Runnable {
    private final DeviceStore store;
    private final CloudClient cloud;
    private final DeviceState state;
    private final ConnectivityMonitor monitor;
    private final long intervalMs;
    private final long spanSamples;
    private final long timeoutMs;
    private final AtomicLongArray liveFrom;
    private final long[] acked = {-1, -1, -1};
    private long ackedSid = -1;
    private long syncedLossesVersion = -1;
    private long syncedSessionsVersion = -1;
    private long lastResultsSyncMono;
    private long lastPersistMono;
    private volatile boolean running = true;

    public LiveUploader(DeviceStore store, CloudClient cloud, DeviceState state, ConnectivityMonitor monitor,
                        long intervalMs, long spanSamples, long timeoutMs, AtomicLongArray liveFrom) {
        this.store = store;
        this.cloud = cloud;
        this.state = state;
        this.monitor = monitor;
        this.intervalMs = intervalMs;
        this.spanSamples = spanSamples;
        this.timeoutMs = timeoutMs;
        this.liveFrom = liveFrom;
    }

    public void stop() {
        running = false;
    }

    public long acked(int sensor) {
        return acked[sensor];
    }

    @Override
    public void run() {
        while (running) {
            long start = Clocks.monoMs();
            if (state.online() && state.sid() >= 0) {
                try {
                    cycle();
                } catch (IOException e) {
                    state.setLastError("live upload: " + e.getMessage());
                    monitor.reportFailure();
                } catch (SQLException | RuntimeException e) {
                    state.setLastError("live upload: " + e);
                }
            }
            Clocks.sleepMs(start + intervalMs - Clocks.monoMs());
        }
    }

    void cycle() throws IOException, SQLException {
        DeviceState.SessionRef ref = state.session();
        long sid = ref.sid();
        String sessionId = ref.sessionId();
        if (sid != ackedSid) {
            ackedSid = sid;
            acked[1] = -1;
            acked[2] = -1;
        }
        long lv = state.lossesVersion.get();
        long sv = state.sessionsVersion.get();
        if (lv != syncedLossesVersion || sv != syncedSessionsVersion) {
            for (DeviceStore.SessionInfo s : recentSessions()) {
                SyncSupport.syncLosses(store, cloud, s, timeoutMs);
                SyncSupport.syncMeta(store, cloud, s, timeoutMs);
            }
            syncedLossesVersion = lv;
            syncedSessionsVersion = sv;
        }
        List<SampleRun> runs = new ArrayList<>();
        long[] heads = new long[3];
        for (int s = 1; s <= 2; s++) {
            long head = state.committedNext.get(s) - 1;
            heads[s] = head;
            long from = Math.max(0, Math.max(acked[s] + 1, head - spanSamples + 1));
            liveFrom.set(s, from);
            if (from <= head) {
                runs.addAll(store.runs(sid, s, from, head));
            }
        }
        if (!runs.isEmpty()) {
            byte[] body = BatchCodec.encode(new BatchCodec.Batch(sessionId, Clocks.wallMs(), runs));
            JsonNode resp = cloud.ingest(body, timeoutMs);
            long samples = 0;
            for (SampleRun r : runs) {
                samples += r.count();
            }
            state.uploadedSamples.addAndGet(samples);
            if (state.sid() == sid) {
                state.cloudHeld.set(1, resp.path("held_1").asLong());
                state.cloudHeld.set(2, resp.path("held_2").asLong());
            }
        }
        for (int s = 1; s <= 2; s++) {
            acked[s] = Math.max(acked[s], heads[s]);
        }
        long now = Clocks.monoMs();
        if (now - lastResultsSyncMono > 5000) {
            for (DeviceStore.SessionInfo s : recentSessions()) {
                SyncSupport.syncResults(store, cloud, s, state, timeoutMs);
            }
            lastResultsSyncMono = now;
        }
        if (now - lastPersistMono > 30_000) {
            store.updateSync(sid, state.cloudHeld.get(1), state.cloudHeld.get(2), null, null);
            lastPersistMono = now;
        }
    }

    private List<DeviceStore.SessionInfo> recentSessions() throws SQLException {
        List<DeviceStore.SessionInfo> all = store.sessions();
        return all.subList(Math.max(0, all.size() - 3), all.size());
    }
}
