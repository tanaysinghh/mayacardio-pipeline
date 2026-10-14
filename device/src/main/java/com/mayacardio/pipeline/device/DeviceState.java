package com.mayacardio.pipeline.device;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public final class DeviceState {
    public static final String ONLINE = "online";
    public static final String OFFLINE = "offline";

    public record SessionRef(long sid, String sessionId, boolean closed) {
    }

    private volatile SessionRef session = new SessionRef(-1, null, false);
    public final AtomicLongArray committedNext = new AtomicLongArray(3);
    public final AtomicLongArray storedCount = new AtomicLongArray(3);
    public final AtomicLongArray cloudHeld = new AtomicLongArray(3);
    public final AtomicLongArray maxCommitGapMs = new AtomicLongArray(3);
    public final AtomicLongArray lastCommitMono = new AtomicLongArray(3);
    public final AtomicLong lossesVersion = new AtomicLong();
    public final AtomicLong sessionsVersion = new AtomicLong();
    public final AtomicLong repliesOk = new AtomicLong();
    public final AtomicLong corruptReplies = new AtomicLong();
    public final AtomicLong timeouts = new AtomicLong();
    public final AtomicLong reconnects = new AtomicLong();
    public final AtomicLong staleReplies = new AtomicLong();
    public final AtomicLong overlapSamplesIgnored = new AtomicLong();
    public final AtomicLong storageErrors = new AtomicLong();
    public final AtomicLong pendingSamples = new AtomicLong();
    public final AtomicLong bytesSent = new AtomicLong();
    public final AtomicLong uploadedSamples = new AtomicLong();
    public final AtomicLong offlineWindows = new AtomicLong();
    private volatile boolean diskFull;
    private volatile boolean mockConnected;
    private volatile String mode = OFFLINE;
    private volatile long modeSinceMono = Clocks.monoMs();
    private volatile String modeSinceWall = Clocks.wallIso();
    private volatile String lastError;

    public synchronized void setSession(long sid, String sessionId, boolean closed) {
        this.session = new SessionRef(sid, sessionId, closed);
        sessionsVersion.incrementAndGet();
    }

    public SessionRef session() {
        return session;
    }

    public long sid() {
        return session.sid();
    }

    public String sessionId() {
        return session.sessionId();
    }

    public boolean sessionClosed() {
        return session.closed();
    }

    public boolean diskFull() {
        return diskFull;
    }

    public void setDiskFull(boolean v) {
        diskFull = v;
    }

    public boolean mockConnected() {
        return mockConnected;
    }

    public void setMockConnected(boolean v) {
        mockConnected = v;
    }

    public String mode() {
        return mode;
    }

    public boolean online() {
        return ONLINE.equals(mode);
    }

    public synchronized void setMode(String m) {
        if (!m.equals(mode)) {
            mode = m;
            modeSinceMono = Clocks.monoMs();
            modeSinceWall = Clocks.wallIso();
        }
    }

    public long modeSinceMono() {
        return modeSinceMono;
    }

    public String modeSinceWall() {
        return modeSinceWall;
    }

    public String lastError() {
        return lastError;
    }

    public void setLastError(String e) {
        lastError = e;
    }

    public void recordCommit(int sensor) {
        long now = Clocks.monoMs();
        long prev = lastCommitMono.getAndSet(sensor, now);
        if (prev > 0) {
            long gap = now - prev;
            maxCommitGapMs.accumulateAndGet(sensor, gap, Math::max);
        }
    }
}
