package com.mayacardio.pipeline.device;

import com.mayacardio.pipeline.device.collector.Collector;
import com.mayacardio.pipeline.device.collector.DiskGuard;
import com.mayacardio.pipeline.device.collector.MockClient;
import com.mayacardio.pipeline.device.net.CloudClient;
import com.mayacardio.pipeline.device.net.ConnectivityMonitor;
import com.mayacardio.pipeline.device.process.OfflineProcessor;
import com.mayacardio.pipeline.device.store.DeviceStore;
import com.mayacardio.pipeline.device.sync.CatchUpUploader;
import com.mayacardio.pipeline.device.sync.LiveUploader;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;

public final class Daemon {
    private final DeviceConfig cfg;
    private final List<Runnable> stoppers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private final List<DeviceStore> stores = new ArrayList<>();

    public Daemon(DeviceConfig cfg) {
        this.cfg = cfg;
    }

    public static FileLock lock(Path dataDir) throws IOException {
        Files.createDirectories(dataDir);
        FileChannel ch = FileChannel.open(dataDir.resolve("pipeline.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock = ch.tryLock();
        if (lock == null) {
            ch.close();
        }
        return lock;
    }

    private Thread start(String name, Runnable r) {
        Thread t = new Thread(r, name);
        t.setDaemon(false);
        t.start();
        threads.add(t);
        return t;
    }

    private DeviceStore open() throws Exception {
        DeviceStore s = DeviceStore.open(cfg.dbPath());
        stores.add(s);
        return s;
    }

    public void run() throws Exception {
        Path dataDir = cfg.dataDir();
        FileLock lock = lock(dataDir);
        if (lock == null) {
            System.err.println("another pipeline daemon is already running on " + dataDir);
            System.exit(3);
        }
        Files.createDirectories(cfg.runDir());
        DeviceState state = new DeviceState();
        DeviceStore collectorStore = open();
        collectorStore.event("daemon_start", "pid=" + ProcessHandle.current().pid());
        System.out.println(Clocks.wallIso() + " pipeline starting: mock " + cfg.mockHost() + ":" + cfg.mockPort()
                + ", data " + dataDir.toAbsolutePath() + ", cloud " + (cfg.cloudUrl().isEmpty() ? "(none: offline only)" : cfg.cloudUrl()));
        DiskGuard disk = new DiskGuard(dataDir, cfg.reserveBytes(), cfg.minFreeBytes(), cfg.resumeFreeBytes(), DiskGuard.REAL);
        Collector collector = new Collector(collectorStore, state,
                new MockClient(cfg.mockHost(), cfg.mockPort(), cfg.replyTimeoutMs()), disk,
                cfg.pollIntervalMs(), cfg.memoryBufferSamples());
        Thread collectorThread = start("collector", collector);
        stoppers.add(collector::stop);

        AtomicLongArray liveFrom = new AtomicLongArray(3);
        LiveUploader live = null;
        CatchUpUploader catchup = null;
        ConnectivityMonitor monitor = null;
        if (!cfg.cloudUrl().isEmpty()) {
            CloudClient heartbeatClient = new CloudClient(cfg.cloudUrl(), cfg.cloudToken(), state.bytesSent);
            CloudClient uploadClient = new CloudClient(cfg.cloudUrl(), cfg.cloudToken(), state.bytesSent);
            monitor = new ConnectivityMonitor(heartbeatClient, state, open(), cfg.heartbeatIntervalMs(),
                    cfg.heartbeatTimeoutMs(), cfg.heartbeatFailThreshold(), cfg.heartbeatOkThreshold());
            live = new LiveUploader(open(), uploadClient, state, monitor, cfg.liveIntervalMs(), cfg.liveSpanSamples(),
                    cfg.uploadTimeoutMs(), liveFrom);
            catchup = new CatchUpUploader(cfg.dbPath(), open(), uploadClient, state, monitor, cfg.catchupChunkSamples(),
                    cfg.catchupParallel(), Math.max(cfg.uploadTimeoutMs(), 60_000), liveFrom);
        }
        LiveUploader liveRef = live;
        OfflineProcessor offline = new OfflineProcessor(open(), state,
                () -> liveRef == null ? -1 : liveRef.acked(1), () -> liveRef == null ? -1 : liveRef.acked(2));
        CatchUpUploader catchupRef = catchup;
        StatusWriter status = new StatusWriter(open(), state, cfg.runDir().resolve("status.json"), cfg.statusIntervalMs(),
                () -> catchupRef == null ? 0 : catchupRef.backlogSamples());
        if (monitor != null) {
            monitor.onModeChange(offline::onModeChange);
            monitor.onModeChange(m -> status.poke());
            if (catchup != null) {
                monitor.onModeChange(m -> {
                    if (DeviceState.ONLINE.equals(m)) {
                        catchupRef.kick();
                    }
                });
            }
            start("heartbeat", monitor);
            stoppers.add(monitor::stop);
            start("live-upload", live);
            stoppers.add(live::stop);
            start("catch-up", catchup);
            stoppers.add(catchup::stop);
        }
        start("offline-processor", offline);
        stoppers.add(offline::stop);
        start("status", status);
        stoppers.add(status::stop);

        java.util.concurrent.atomic.AtomicBoolean stopping = new java.util.concurrent.atomic.AtomicBoolean();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stopping.set(true);
            for (Runnable r : stoppers) {
                r.run();
            }
            for (Thread t : threads) {
                try {
                    t.join(5000);
                } catch (InterruptedException ignored) {
                }
            }
            for (DeviceStore s : stores) {
                s.close();
            }
        }, "shutdown"));
        collectorThread.join();
        if (!stopping.get()) {
            System.err.println(Clocks.wallIso() + " collector stopped unexpectedly: " + state.lastError());
            Runtime.getRuntime().halt(1);
        }
        lock.release();
    }
}
