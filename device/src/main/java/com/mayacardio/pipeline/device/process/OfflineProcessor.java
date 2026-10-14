package com.mayacardio.pipeline.device.process;

import com.mayacardio.pipeline.core.MemorySignalView;
import com.mayacardio.pipeline.core.ResultJson;
import com.mayacardio.pipeline.core.SampleBlock;
import com.mayacardio.pipeline.core.Signal;
import com.mayacardio.pipeline.core.SignalView;
import com.mayacardio.pipeline.core.WindowProcessor;
import com.mayacardio.pipeline.core.WindowResult;
import com.mayacardio.pipeline.device.Clocks;
import com.mayacardio.pipeline.device.DeviceState;
import com.mayacardio.pipeline.device.store.DeviceStore;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

public final class OfflineProcessor implements Runnable {
    private static final int BATCH = 50;

    private final DeviceStore store;
    private final DeviceState state;
    private final LongSupplier[] liveAcked;
    private final Object wake = new Object();
    private volatile boolean running = true;
    private volatile boolean restartFromAcked = true;
    private volatile boolean drainRequested;

    public OfflineProcessor(DeviceStore store, DeviceState state, LongSupplier ackedSensor1, LongSupplier ackedSensor2) {
        this.store = store;
        this.state = state;
        this.liveAcked = new LongSupplier[]{null, ackedSensor1, ackedSensor2};
    }

    public void stop() {
        running = false;
        synchronized (wake) {
            wake.notifyAll();
        }
    }

    public void onModeChange(String mode) {
        if (DeviceState.OFFLINE.equals(mode)) {
            restartFromAcked = true;
        } else {
            drainRequested = true;
        }
        synchronized (wake) {
            wake.notifyAll();
        }
    }

    @Override
    public void run() {
        while (running) {
            try {
                if (!state.online()) {
                    DeviceState.SessionRef ref = state.session();
                    if (restartFromAcked || ref.sid() >= 0 && !ref.closed() && store.offlineNext(ref.sid()) == null) {
                        startCurrentSession();
                        restartFromAcked = false;
                    }
                    process(false);
                } else if (drainRequested) {
                    drainRequested = false;
                    process(true);
                }
            } catch (SQLException | RuntimeException e) {
                state.setLastError("offline processing: " + e);
            }
            synchronized (wake) {
                try {
                    wake.wait(1000);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    void startCurrentSession() throws SQLException {
        DeviceState.SessionRef ref = state.session();
        if (ref.sid() < 0 || ref.closed()) {
            return;
        }
        long acked = Math.min(liveAcked[1].getAsLong(), liveAcked[2].getAsLong());
        long from = acked >= 0 ? acked + 1 : Math.min(state.committedNext.get(1), state.committedNext.get(2));
        long start = Math.max(0, Signal.windowOf(from) - 1);
        store.setOfflineNext(ref.sid(), start);
    }

    void process(boolean finalDrain) throws SQLException {
        DeviceState.SessionRef ref = state.session();
        for (DeviceStore.SessionInfo s : store.sessions()) {
            Long next = store.offlineNext(s.sid());
            if (next == null) {
                continue;
            }
            boolean current = s.sid() == ref.sid() && !ref.closed() && !s.closed();
            long end1 = current ? state.committedNext.get(1) : store.stream(s.sid(), 1).nextIndex();
            long end2 = current ? state.committedNext.get(2) : store.stream(s.sid(), 2).nextIndex();
            long lastWindow = Signal.windowOf(Math.max(end1, end2) - 1);
            DbView view = new DbView(store, s.sid(), end1, end2, !current);
            List<DeviceStore.StoredResult> batch = new ArrayList<>();
            long w = next;
            while (w <= lastWindow) {
                WindowResult r = WindowProcessor.compute(view, w);
                if (r == null) {
                    break;
                }
                String body = ResultJson.line(s.sessionId(), ResultJson.OFFLINE, r,
                        ResultJson.timestamps("computed_at", Clocks.wallIso()));
                batch.add(new DeviceStore.StoredResult(w, ResultJson.OFFLINE, body));
                w++;
                if (batch.size() >= BATCH) {
                    flush(s.sid(), batch, w);
                }
            }
            flush(s.sid(), batch, w);
            if (!current && (finalDrain || w > lastWindow)) {
                store.clearOfflineNext(s.sid());
            }
        }
    }

    private void flush(long sid, List<DeviceStore.StoredResult> batch, long next) throws SQLException {
        if (batch.isEmpty()) {
            return;
        }
        store.insertResults(sid, batch, next);
        state.offlineWindows.addAndGet(batch.size());
        batch.clear();
    }

    static final class DbView implements SignalView {
        private final DeviceStore store;
        private final long sid;
        private final long[] end;
        private final boolean closed;

        DbView(DeviceStore store, long sid, long end1, long end2, boolean closed) {
            this.store = store;
            this.sid = sid;
            this.end = new long[]{0, end1, end2};
            this.closed = closed;
        }

        @Override
        public boolean isResolved(int sensor, long from, long to) {
            long sessionEnd = closed ? end[sensor] : Long.MAX_VALUE;
            return MemorySignalView.prefixResolved(end[sensor], sessionEnd, from, to);
        }

        @Override
        public SampleBlock load(int sensor, long from, long to) {
            try {
                return store.block(sid, sensor, from, to);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
