package com.mayacardio.pipeline.device;

import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.device.store.DeviceStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public final class StatusWriter implements Runnable {
    private final DeviceStore store;
    private final DeviceState state;
    private final Path file;
    private final long intervalMs;
    private final LongSupplier catchupBacklog;
    private final Object wake = new Object();
    private volatile boolean running = true;
    private long cachedSessionsVersion = -1;
    private long closedSaved;
    private long closedCloud;
    private long lossRanges;
    private long cachedLossesVersion = -1;
    private long lastRefreshMono;

    public StatusWriter(DeviceStore store, DeviceState state, Path file, long intervalMs, LongSupplier catchupBacklog) {
        this.store = store;
        this.state = state;
        this.file = file;
        this.intervalMs = intervalMs;
        this.catchupBacklog = catchupBacklog;
    }

    public void poke() {
        synchronized (wake) {
            wake.notifyAll();
        }
    }

    public void stop() {
        running = false;
        poke();
    }

    @Override
    public void run() {
        while (running) {
            try {
                write("running");
            } catch (IOException | SQLException | RuntimeException e) {
                state.setLastError("status: " + e.getMessage());
            }
            synchronized (wake) {
                try {
                    wake.wait(intervalMs);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
        try {
            write("stopped");
        } catch (IOException | SQLException | RuntimeException ignored) {
        }
    }

    private void refreshTotals(DeviceState.SessionRef ref) throws SQLException {
        long sv = state.sessionsVersion.get();
        long lv = state.lossesVersion.get();
        boolean stale = Clocks.monoMs() - lastRefreshMono > 10_000;
        if (sv != cachedSessionsVersion || stale) {
            lastRefreshMono = Clocks.monoMs();
            long saved = 0;
            long cloud = 0;
            for (DeviceStore.SessionInfo s : store.sessions()) {
                if (s.sid() == ref.sid()) {
                    continue;
                }
                saved += store.stream(s.sid(), 1).storedCount() + store.stream(s.sid(), 2).storedCount();
                long[] sync = store.syncState(s.sid());
                cloud += sync[0] + sync[1];
            }
            closedSaved = saved;
            closedCloud = cloud;
            cachedSessionsVersion = sv;
        }
        if (lv != cachedLossesVersion || sv != cachedSessionsVersion) {
            long n = 0;
            for (DeviceStore.SessionInfo s : store.sessions()) {
                n += store.lossCount(s.sid());
            }
            lossRanges = n;
            cachedLossesVersion = lv;
        }
    }

    void write(String daemon) throws IOException, SQLException {
        DeviceState.SessionRef ref = state.session();
        refreshTotals(ref);
        long saved1 = state.storedCount.get(1);
        long saved2 = state.storedCount.get(2);
        long cloud1 = Math.min(state.cloudHeld.get(1), saved1);
        long cloud2 = Math.min(state.cloudHeld.get(2), saved2);
        long saved = closedSaved + saved1 + saved2;
        long cloud = closedCloud + cloud1 + cloud2;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mode", state.mode());
        m.put("daemon", daemon);
        m.put("pid", ProcessHandle.current().pid());
        m.put("session", ref.sessionId());
        m.put("samples_saved", saved);
        m.put("samples_in_cloud", cloud);
        m.put("backlog", Math.max(0, saved - cloud));
        Map<String, Object> cur = new LinkedHashMap<>();
        cur.put("saved_1", saved1);
        cur.put("saved_2", saved2);
        cur.put("cloud_1", cloud1);
        cur.put("cloud_2", cloud2);
        cur.put("next_index_1", state.committedNext.get(1));
        cur.put("next_index_2", state.committedNext.get(2));
        cur.put("closed", ref.closed());
        m.put("current_session", cur);
        m.put("catchup_backlog_samples", catchupBacklog.getAsLong());
        m.put("loss_ranges", lossRanges);
        m.put("mode_since", state.modeSinceWall());
        m.put("mock_connected", state.mockConnected());
        m.put("disk_full", state.diskFull());
        m.put("pending_in_memory", state.pendingSamples.get());
        m.put("max_commit_gap_ms", List.of(state.maxCommitGapMs.get(1), state.maxCommitGapMs.get(2)));
        m.put("replies_ok", state.repliesOk.get());
        m.put("corrupt_replies", state.corruptReplies.get());
        m.put("timeouts", state.timeouts.get());
        m.put("reconnects", state.reconnects.get());
        m.put("stale_replies", state.staleReplies.get());
        m.put("storage_errors", state.storageErrors.get());
        m.put("offline_windows", state.offlineWindows.get());
        m.put("uploaded_samples", state.uploadedSamples.get());
        m.put("http_bytes_sent", state.bytesSent.get());
        m.put("last_error", state.lastError());
        m.put("updated_at", Clocks.wallIso());
        writeAtomically(file, Json.write(m));
    }

    static void writeAtomically(Path file, String json) throws IOException {
        Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(tmp, json + "\n", StandardCharsets.UTF_8);
        try {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
