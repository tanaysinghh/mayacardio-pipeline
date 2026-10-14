package com.mayacardio.pipeline.cloud.service;

import com.mayacardio.pipeline.cloud.store.CloudStore;
import com.mayacardio.pipeline.core.ResultJson;
import com.mayacardio.pipeline.core.SignalView;
import com.mayacardio.pipeline.core.WindowProcessor;
import com.mayacardio.pipeline.core.WindowResult;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ProcessingService implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(ProcessingService.class.getName());
    private static final int FLUSH_EVERY = 200;

    private final SessionRegistry registry;
    private final Set<SessionState> dirty = new LinkedHashSet<>();
    private final Thread worker;
    private volatile boolean running = true;
    private final AtomicLong windowsComputed = new AtomicLong();

    public ProcessingService(SessionRegistry registry) {
        this.registry = registry;
        this.worker = new Thread(this::loop, "window-processor");
        this.worker.setDaemon(true);
        this.worker.start();
    }

    public void signal(SessionState st) {
        synchronized (dirty) {
            dirty.add(st);
            dirty.notifyAll();
        }
    }

    public long windowsComputed() {
        return windowsComputed.get();
    }

    private void loop() {
        while (running) {
            List<SessionState> work;
            synchronized (dirty) {
                while (dirty.isEmpty() && running) {
                    try {
                        dirty.wait(5000);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                work = new ArrayList<>(dirty);
                dirty.clear();
            }
            for (SessionState st : work) {
                try {
                    process(st);
                } catch (RuntimeException e) {
                    LOG.log(Level.WARNING, "processing failed for " + st.id() + ", will retry", e);
                    sleep(1000);
                    signal(st);
                }
            }
        }
    }

    public void process(SessionState st) {
        SignalView view = registry.view(st);
        List<CloudStore.StoredResult> batch = new ArrayList<>();
        List<Long> done = new ArrayList<>();
        for (long w : st.pendingSnapshot()) {
            WindowResult r = WindowProcessor.compute(view, w);
            if (r == null) {
                continue;
            }
            String now = Instant.now().toString();
            String body = ResultJson.line(st.id(), ResultJson.ONLINE, r, ResultJson.timestamps(
                    "computed_at", now,
                    "data_received_at", st.lastIngestWall(),
                    "device_captured_at", st.lastCapturedAtMs() > 0 ? Instant.ofEpochMilli(st.lastCapturedAtMs()).toString() : null));
            batch.add(new CloudStore.StoredResult(w, ResultJson.ONLINE, body));
            done.add(w);
            if (batch.size() >= FLUSH_EVERY) {
                flush(st, batch, done);
            }
        }
        flush(st, batch, done);
    }

    private void flush(SessionState st, List<CloudStore.StoredResult> batch, List<Long> done) {
        if (batch.isEmpty()) {
            return;
        }
        registry.store().upsertResults(st.id(), batch, Instant.now().toString());
        for (long w : done) {
            st.markComputed(w);
        }
        windowsComputed.addAndGet(done.size());
        batch.clear();
        done.clear();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running = false;
        worker.interrupt();
    }
}
