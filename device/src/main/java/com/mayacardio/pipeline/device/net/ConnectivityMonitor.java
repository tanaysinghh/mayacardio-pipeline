package com.mayacardio.pipeline.device.net;

import com.mayacardio.pipeline.device.Clocks;
import com.mayacardio.pipeline.device.DeviceState;
import com.mayacardio.pipeline.device.store.DeviceStore;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public final class ConnectivityMonitor implements Runnable {
    private final CloudClient client;
    private final DeviceState state;
    private final DeviceStore store;
    private final long intervalMs;
    private final long timeoutMs;
    private final int failThreshold;
    private final int okThreshold;
    private final List<Consumer<String>> listeners = new CopyOnWriteArrayList<>();
    private final Object wake = new Object();
    private volatile boolean running = true;
    private volatile boolean urgent;
    private int fails;
    private int oks;

    public ConnectivityMonitor(CloudClient client, DeviceState state, DeviceStore store, long intervalMs, long timeoutMs,
                               int failThreshold, int okThreshold) {
        this.client = client;
        this.state = state;
        this.store = store;
        this.intervalMs = intervalMs;
        this.timeoutMs = timeoutMs;
        this.failThreshold = failThreshold;
        this.okThreshold = okThreshold;
    }

    public void onModeChange(Consumer<String> listener) {
        listeners.add(listener);
    }

    public void reportFailure() {
        urgent = true;
        synchronized (wake) {
            wake.notifyAll();
        }
    }

    public void stop() {
        running = false;
        synchronized (wake) {
            wake.notifyAll();
        }
    }

    @Override
    public void run() {
        long nextAt = Clocks.monoMs();
        while (running) {
            long start = Clocks.monoMs();
            boolean ok = client.health(timeoutMs);
            observe(ok);
            nextAt = Math.max(nextAt + intervalMs, start + 200);
            synchronized (wake) {
                long wait = nextAt - Clocks.monoMs();
                while (running && wait > 0 && !urgent) {
                    try {
                        wake.wait(wait);
                    } catch (InterruptedException e) {
                        return;
                    }
                    wait = nextAt - Clocks.monoMs();
                }
            }
            if (urgent) {
                urgent = false;
                nextAt = Clocks.monoMs();
            }
        }
    }

    void observe(boolean ok) {
        if (ok) {
            oks++;
            fails = 0;
        } else {
            fails++;
            oks = 0;
        }
        String mode = state.mode();
        if (DeviceState.OFFLINE.equals(mode) && oks >= okThreshold) {
            switchTo(DeviceState.ONLINE);
        } else if (DeviceState.ONLINE.equals(mode) && fails >= failThreshold) {
            switchTo(DeviceState.OFFLINE);
        }
    }

    private void switchTo(String mode) {
        state.setMode(mode);
        store.event("mode", mode);
        System.out.println(Clocks.wallIso() + " mode -> " + mode);
        for (Consumer<String> l : listeners) {
            try {
                l.accept(mode);
            } catch (RuntimeException ignored) {
            }
        }
    }
}
