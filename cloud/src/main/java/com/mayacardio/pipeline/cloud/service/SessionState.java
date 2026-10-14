package com.mayacardio.pipeline.cloud.service;

import com.mayacardio.pipeline.cloud.store.Segment;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.RangeSet;
import com.mayacardio.pipeline.core.Signal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public final class SessionState {
    private static final int CACHE_SEGMENTS = 512;

    private final String id;
    private final RangeSet[] held = {null, new RangeSet(), new RangeSet()};
    private final RangeSet[] lost = {null, new RangeSet(), new RangeSet()};
    private final Long[] finalNext = new Long[3];
    private final Object[] locks = {null, new Object(), new Object()};
    @SuppressWarnings("unchecked")
    private final Map<Long, Segment>[] cache = new Map[3];
    private final TreeSet<Long> pending = new TreeSet<>();
    private final TreeSet<Long> computed = new TreeSet<>();
    private long maxWindow = -1;
    private boolean closed;
    private volatile String lastIngestWall;
    private volatile long lastCapturedAtMs;
    private volatile long conflicts;

    public SessionState(String id) {
        this.id = id;
        for (int s = 1; s <= 2; s++) {
            cache[s] = new LinkedHashMap<>(CACHE_SEGMENTS, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, Segment> eldest) {
                    return size() > CACHE_SEGMENTS;
                }
            };
        }
    }

    public String id() {
        return id;
    }

    public Object lock(int sensor) {
        return locks[sensor];
    }

    public Map<Long, Segment> cache(int sensor) {
        return cache[sensor];
    }

    public synchronized void addHeld(int sensor, long from, long to) {
        held[sensor].add(from, to);
        extendWindows(Signal.windowOf(to));
    }

    public synchronized void addLoss(LossRange l) {
        long end = l.endInclusive();
        lost[l.sensor()].add(l.fromIndex(), end);
        if (end != Long.MAX_VALUE) {
            extendWindows(Signal.windowOf(end));
        }
    }

    public synchronized void setClosed(boolean closed, Long next1, Long next2) {
        this.closed = closed;
        finalNext[1] = next1;
        finalNext[2] = next2;
    }

    public synchronized boolean closed() {
        return closed;
    }

    private void extendWindows(long w) {
        while (maxWindow < w) {
            maxWindow++;
            if (!computed.contains(maxWindow)) {
                pending.add(maxWindow);
            }
        }
    }

    public synchronized void markComputed(long w) {
        pending.remove(w);
        computed.add(w);
    }

    public synchronized void initComputed(java.util.Collection<Long> windows) {
        computed.addAll(windows);
        pending.removeAll(windows);
    }

    public synchronized List<Long> pendingSnapshot() {
        return new ArrayList<>(pending);
    }

    public synchronized int pendingCount() {
        return pending.size();
    }

    public synchronized int computedCount() {
        return computed.size();
    }

    public synchronized boolean isResolved(int sensor, long from, long to) {
        if (to < from) {
            return true;
        }
        long f = from;
        while (f <= to) {
            if (finalNext[sensor] != null && f >= finalNext[sensor]) {
                return true;
            }
            Long h = held[sensor].rangeEnd(f);
            Long l = lost[sensor].rangeEnd(f);
            long end = Math.max(h == null ? f - 1 : h, l == null ? f - 1 : l);
            if (end < f) {
                return false;
            }
            if (end == Long.MAX_VALUE) {
                return true;
            }
            f = end + 1;
        }
        return true;
    }

    public synchronized long heldCount(int sensor) {
        return held[sensor].count();
    }

    public synchronized List<long[]> heldRanges(int sensor) {
        return held[sensor].toList();
    }

    public String lastIngestWall() {
        return lastIngestWall;
    }

    public long lastCapturedAtMs() {
        return lastCapturedAtMs;
    }

    public void noteIngest(String wall, long capturedAtMs) {
        lastIngestWall = wall;
        lastCapturedAtMs = capturedAtMs;
    }

    public void addConflicts(long n) {
        conflicts += n;
    }

    public long conflicts() {
        return conflicts;
    }
}
