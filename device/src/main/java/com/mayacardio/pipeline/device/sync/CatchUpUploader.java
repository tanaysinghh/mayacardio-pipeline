package com.mayacardio.pipeline.device.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.BatchCodec;
import com.mayacardio.pipeline.core.RangeSet;
import com.mayacardio.pipeline.core.SampleRun;
import com.mayacardio.pipeline.device.Clocks;
import com.mayacardio.pipeline.device.DeviceState;
import com.mayacardio.pipeline.device.net.CloudClient;
import com.mayacardio.pipeline.device.net.ConnectivityMonitor;
import com.mayacardio.pipeline.device.store.DeviceStore;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public final class CatchUpUploader implements Runnable {
    private static final long IDLE_RECHECK_MS = 5000;
    private static final long LONG_OUTAGE_MS = 60_000;
    private static final long WAKE_TIMEOUT_MS = 120_000;
    private static final long MAX_BACKOFF_MS = 30_000;
    private static final int MIN_CHUNK = 4000;

    private final Path dbPath;
    private final DeviceStore store;
    private final CloudClient cloud;
    private final DeviceState state;
    private final ConnectivityMonitor monitor;
    private final int chunkSamples;
    private volatile int currentChunk;
    private volatile long offlineSinceMono = -1;
    private volatile boolean wakeTimeouts;
    private long backoffMs;
    private final int parallel;
    private final long timeoutMs;
    private final AtomicLongArray liveFrom;
    private final Object wake = new Object();
    private final AtomicLong backlogSamples = new AtomicLong();
    private volatile boolean running = true;
    private volatile boolean kick = true;
    private volatile long kickedAtMono;
    private ExecutorService pool;
    private ThreadLocal<DeviceStore> readers;
    private final List<DeviceStore> openReaders = new ArrayList<>();

    public CatchUpUploader(Path dbPath, DeviceStore store, CloudClient cloud, DeviceState state, ConnectivityMonitor monitor,
                           int chunkSamples, int parallel, long timeoutMs, AtomicLongArray liveFrom) {
        this.dbPath = dbPath;
        this.store = store;
        this.cloud = cloud;
        this.state = state;
        this.monitor = monitor;
        this.chunkSamples = chunkSamples;
        this.currentChunk = chunkSamples;
        this.parallel = Math.max(1, parallel);
        this.timeoutMs = timeoutMs;
        this.liveFrom = liveFrom;
    }

    public long backlogSamples() {
        return backlogSamples.get();
    }

    public void onModeChange(String mode) {
        if (DeviceState.OFFLINE.equals(mode)) {
            offlineSinceMono = Clocks.monoMs();
        } else {
            if (offlineSinceMono >= 0 && Clocks.monoMs() - offlineSinceMono > LONG_OUTAGE_MS) {
                wakeTimeouts = true;
            }
            offlineSinceMono = -1;
            backoffMs = 0;
            kick();
        }
    }

    private long timeout() {
        return wakeTimeouts ? Math.max(timeoutMs, WAKE_TIMEOUT_MS) : timeoutMs;
    }

    public void kick() {
        kickedAtMono = Clocks.monoMs();
        kick = true;
        synchronized (wake) {
            wake.notifyAll();
        }
    }

    public void stop() {
        running = false;
        kick();
    }

    @Override
    public void run() {
        pool = Executors.newFixedThreadPool(parallel, r -> {
            Thread t = new Thread(r, "catchup-worker");
            t.setDaemon(true);
            return t;
        });
        readers = ThreadLocal.withInitial(() -> {
            try {
                DeviceStore s = DeviceStore.open(dbPath);
                synchronized (openReaders) {
                    openReaders.add(s);
                }
                return s;
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        while (running) {
            boolean more = false;
            if (state.online()) {
                try {
                    more = pass();
                    wakeTimeouts = false;
                    backoffMs = 0;
                } catch (IOException e) {
                    state.setLastError("catch-up: " + e.getMessage());
                    System.out.println(Clocks.wallIso() + " catch-up failed: " + e.getMessage());
                    if (String.valueOf(e.getMessage()).contains("timed out")) {
                        currentChunk = Math.max(MIN_CHUNK, currentChunk / 2);
                    }
                    backoffMs = backoffMs == 0 ? 1000 : Math.min(MAX_BACKOFF_MS, backoffMs * 2);
                    monitor.reportFailure();
                } catch (SQLException | RuntimeException e) {
                    state.setLastError("catch-up: " + e);
                }
            }
            if (!more) {
                synchronized (wake) {
                    if (!kick && running) {
                        try {
                            boolean recentKick = Clocks.monoMs() - kickedAtMono < 15_000;
                            long wait = backoffMs > 0 ? backoffMs : recentKick ? 1000 : IDLE_RECHECK_MS;
                            wake.wait(wait);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                    kick = false;
                }
            }
        }
        pool.shutdownNow();
        synchronized (openReaders) {
            for (DeviceStore s : openReaders) {
                s.close();
            }
        }
    }

    boolean pass() throws IOException, SQLException {
        DeviceState.SessionRef current = state.session();
        long totalBacklog = 0;
        boolean sentSomething = false;
        for (DeviceStore.SessionInfo s : store.sessions()) {
            long[] sync = store.syncState(s.sid());
            if (sync[3] != 0) {
                continue;
            }
            if (!state.online()) {
                return false;
            }
            boolean isCurrent = s.sid() == current.sid() && !s.closed();
            CloudClient.Ranges cloudRanges = cloud.ranges(s.sessionId(), timeout());
            RangeSet[] missing = new RangeSet[3];
            long sessionBacklog = 0;
            for (int sensor = 1; sensor <= 2; sensor++) {
                RangeSet stored = store.storedRuns(s.sid(), sensor);
                missing[sensor] = stored.minus(cloudRanges.held()[sensor]);
                if (isCurrent) {
                    long limit = liveFrom.get(sensor) - 1;
                    missing[sensor] = missing[sensor].clip(0, Math.max(-1, limit));
                }
                sessionBacklog += missing[sensor].count();
            }
            totalBacklog += sessionBacklog;
            backlogSamples.set(totalBacklog);
            if (sessionBacklog > 0) {
                sendMissing(s, missing);
                sentSomething = true;
            }
            SyncSupport.syncLosses(store, cloud, s, timeout());
            SyncSupport.syncMeta(store, cloud, s, timeout());
            SyncSupport.syncResults(store, cloud, s, state, timeout());
            if (s.closed()) {
                CloudClient.Ranges after = cloud.ranges(s.sessionId(), timeout());
                boolean complete = true;
                for (int sensor = 1; sensor <= 2; sensor++) {
                    if (!store.storedRuns(s.sid(), sensor).minus(after.held()[sensor]).isEmpty()) {
                        complete = false;
                    }
                }
                long[] after2 = store.syncState(s.sid());
                if (complete && after2[2] != 0 && store.unsyncedLosses(s.sid()).isEmpty()) {
                    store.updateSync(s.sid(), after.heldCount()[1], after.heldCount()[2], null, true);
                    store.event("cloud_copy_complete", s.sessionId());
                }
            }
        }
        backlogSamples.set(sentSomething ? backlogSamples.get() : totalBacklog);
        return sentSomething;
    }

    private void sendMissing(DeviceStore.SessionInfo s, RangeSet[] missing) throws IOException {
        Long lo = null;
        Long hi = null;
        for (int sensor = 1; sensor <= 2; sensor++) {
            if (!missing[sensor].isEmpty()) {
                lo = lo == null ? missing[sensor].first() : Math.min(lo, missing[sensor].first());
                hi = hi == null ? missing[sensor].last() : Math.max(hi, missing[sensor].last());
            }
        }
        if (lo == null) {
            return;
        }
        List<long[]> chunks = new ArrayList<>();
        long a = lo;
        while (a <= hi) {
            long b = Math.min(hi, a + currentChunk - 1);
            boolean any = false;
            for (int sensor = 1; sensor <= 2; sensor++) {
                any |= missing[sensor].intersects(a, b);
            }
            if (any) {
                chunks.add(new long[]{a, b});
            }
            a = b + 1;
        }
        List<Future<Long>> futures = new ArrayList<>();
        int next = 0;
        IOException failure = null;
        while ((next < chunks.size() || !futures.isEmpty()) && failure == null) {
            while (next < chunks.size() && futures.size() < parallel) {
                long[] c = chunks.get(next++);
                futures.add(pool.submit(() -> sendChunk(s, missing, c[0], c[1])));
            }
            Future<Long> f = futures.remove(0);
            try {
                long sent = f.get();
                backlogSamples.addAndGet(-sent);
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable cause = e.getCause();
                failure = cause instanceof IOException io ? io : new IOException(String.valueOf(cause), cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failure = new IOException("interrupted");
            }
            if (!state.online() && failure == null) {
                failure = new IOException("went offline during catch-up");
            }
        }
        for (Future<Long> f : futures) {
            f.cancel(false);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private long sendChunk(DeviceStore.SessionInfo s, RangeSet[] missing, long a, long b) throws IOException, SQLException {
        DeviceStore reader = readers.get();
        List<SampleRun> runs = new ArrayList<>();
        for (int sensor = 1; sensor <= 2; sensor++) {
            for (long[] r : missing[sensor].clip(a, b)) {
                runs.addAll(reader.runs(s.sid(), sensor, r[0], r[1]));
            }
        }
        if (runs.isEmpty()) {
            return 0;
        }
        byte[] body = BatchCodec.encode(new BatchCodec.Batch(s.sessionId(), Clocks.wallMs(), runs));
        JsonNode resp = cloud.ingest(body, timeout());
        if (currentChunk < chunkSamples) {
            currentChunk = Math.min(chunkSamples, currentChunk * 2);
        }
        long n = 0;
        for (SampleRun r : runs) {
            n += r.count();
        }
        state.uploadedSamples.addAndGet(n);
        DeviceState.SessionRef ref = state.session();
        if (ref.sid() == s.sid()) {
            state.cloudHeld.set(1, resp.path("held_1").asLong());
            state.cloudHeld.set(2, resp.path("held_2").asLong());
        }
        return n;
    }
}
