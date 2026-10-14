package com.mayacardio.pipeline.device.collector;

import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.device.Clocks;
import com.mayacardio.pipeline.device.DeviceState;
import com.mayacardio.pipeline.device.store.DeviceStore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public final class Collector implements Runnable {
    public static final String R_UNDELIVERED = "not_delivered_by_mock";
    public static final String R_SESSION_RESTART = "mock_session_restarted_unread_tail";
    public static final String R_DISK_FULL = "disk_full";
    public static final String R_STORAGE = "storage_error";
    private static final int MAX_TIMEOUTS_BEFORE_RECONNECT = 3;
    private static final int MAX_HINTS = 64;

    private final DeviceStore store;
    private final DeviceState state;
    private final MockClient client;
    private final DiskGuard disk;
    private final long pollIntervalMs;
    private final int memoryCapSamples;
    private volatile boolean running = true;

    private long sid = -1;
    private String sessionId;
    private boolean closed;
    private final long[] next = new long[3];
    private final boolean[] ended = new boolean[3];
    @SuppressWarnings("unchecked")
    private final List<LossRange>[] hints = new List[]{null, new ArrayList<>(), new ArrayList<>()};
    @SuppressWarnings("unchecked")
    private final ArrayDeque<DeviceStore.Commit>[] pending = new ArrayDeque[]{null, new ArrayDeque<>(), new ArrayDeque<>()};
    private final long[] pendingSamples = new long[3];
    private int consecutiveTimeouts;
    private boolean needProbe = true;
    private long reconnectBackoff = 250;
    private int maxCount = 400;

    public Collector(DeviceStore store, DeviceState state, MockClient client, DiskGuard disk,
                     long pollIntervalMs, int memoryCapSamples) {
        this.store = store;
        this.state = state;
        this.client = client;
        this.disk = disk;
        this.pollIntervalMs = pollIntervalMs;
        this.memoryCapSamples = memoryCapSamples;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        try {
            loadState();
        } catch (SQLException e) {
            state.setLastError("cannot load state: " + e.getMessage());
            throw new IllegalStateException(e);
        }
        disk.ensureReserve();
        while (running) {
            try {
                step();
            } catch (RuntimeException e) {
                state.setLastError("collector: " + e);
                store.event("collector_error", String.valueOf(e));
                Clocks.sleepMs(200);
            }
        }
        flushPending();
        client.close();
        state.setMockConnected(false);
    }

    void loadState() throws SQLException {
        DeviceStore.SessionInfo latest = store.latestSession();
        if (latest == null) {
            state.setSession(-1, null, false);
            return;
        }
        sid = latest.sid();
        sessionId = latest.sessionId();
        closed = latest.closed();
        for (int s = 1; s <= 2; s++) {
            DeviceStore.StreamState st = store.stream(sid, s);
            next[s] = st.nextIndex();
            ended[s] = st.ended() || closed;
            state.committedNext.set(s, st.nextIndex());
            state.storedCount.set(s, st.storedCount());
        }
        state.setSession(sid, sessionId, closed);
    }

    void step() {
        if (!client.connected()) {
            state.setMockConnected(false);
            try {
                client.connect();
                state.reconnects.incrementAndGet();
                needProbe = true;
                consecutiveTimeouts = 0;
                reconnectBackoff = 250;
                state.setMockConnected(true);
            } catch (IOException e) {
                flushPending();
                Clocks.sleepMs(reconnectBackoff);
                reconnectBackoff = Math.min(2000, reconnectBackoff * 2);
                return;
            }
        }
        if (needProbe) {
            MockClient.Result r = client.read(1, null, (rep, s, f) -> rep.sensor() != s);
            if (r.outcome() != MockClient.Outcome.OK) {
                handleFailure(r);
                return;
            }
            consecutiveTimeouts = 0;
            onSessionSeen(r.reply().sessionId());
            needProbe = false;
            return;
        }
        if (closed) {
            flushPending();
            Clocks.sleepMs(1000);
            needProbe = true;
            return;
        }
        long cycleStart = Clocks.monoMs();
        boolean more = false;
        for (int s = 1; s <= 2; s++) {
            if (ended[s]) {
                continue;
            }
            MockClient.Result r = client.read(s, next[s], this::stale);
            if (r.outcome() != MockClient.Outcome.OK) {
                handleFailure(r);
                if (r.outcome() == MockClient.Outcome.CORRUPT) {
                    more = true;
                    continue;
                }
                return;
            }
            consecutiveTimeouts = 0;
            MockReply rep = r.reply();
            if (!rep.sessionId().equals(sessionId)) {
                onSessionSeen(rep.sessionId());
                return;
            }
            state.repliesOk.incrementAndGet();
            maxCount = Math.max(maxCount, rep.count());
            process(s, rep);
            if (rep.count() > 0 && rep.count() >= maxCount) {
                more = true;
            }
        }
        if (ended[1] && ended[2] && !closed) {
            finishSession();
        }
        flushPending();
        if (!more) {
            Clocks.sleepMs(cycleStart + pollIntervalMs - Clocks.monoMs());
        }
    }

    private void handleFailure(MockClient.Result r) {
        switch (r.outcome()) {
            case CORRUPT -> {
                state.corruptReplies.incrementAndGet();
                store.event("corrupt_reply", r.detail());
            }
            case TIMEOUT -> {
                state.timeouts.incrementAndGet();
                consecutiveTimeouts++;
                if (consecutiveTimeouts >= MAX_TIMEOUTS_BEFORE_RECONNECT) {
                    store.event("mock_unresponsive", "reconnecting after " + consecutiveTimeouts + " timeouts");
                    client.close();
                }
            }
            case DISCONNECTED -> client.close();
            default -> {
            }
        }
        flushPending();
    }

    boolean stale(MockReply rep, int sensor, Long from) {
        if (rep.sensor() != sensor) {
            state.staleReplies.incrementAndGet();
            return true;
        }
        if (from == null || !rep.sessionId().equals(sessionId)) {
            return false;
        }
        if (rep.firstIndex() + rep.count() > from) {
            return false;
        }
        for (LossRange l : rep.lost()) {
            if (l.endInclusive() >= from) {
                return false;
            }
        }
        state.staleReplies.incrementAndGet();
        return true;
    }

    private void onSessionSeen(String id) {
        if (id.equals(sessionId)) {
            return;
        }
        try {
            if (store.session(id) != null) {
                store.event("session_reappeared", id);
                state.setLastError("mock reported previously closed session " + id);
                client.close();
                Clocks.sleepMs(1000);
                return;
            }
            if (sessionId != null) {
                if (!flushPendingForSwitch()) {
                    Clocks.sleepMs(500);
                    needProbe = true;
                    return;
                }
                if (!closed) {
                    List<LossRange> tail = new ArrayList<>();
                    for (int s = 1; s <= 2; s++) {
                        if (!ended[s]) {
                            tail.add(new LossRange(s, next[s], null, R_SESSION_RESTART));
                        }
                    }
                    store.closeSession(sid, "mock restarted with new session " + id, tail);
                    state.committedNext.set(1, next[1]);
                    state.committedNext.set(2, next[2]);
                    state.lossesVersion.incrementAndGet();
                }
                store.event("session_change", sessionId + " -> " + id);
            }
            long newSid = store.createSession(id);
            sid = newSid;
            sessionId = id;
            closed = false;
            for (int s = 1; s <= 2; s++) {
                next[s] = 0;
                ended[s] = false;
                hints[s].clear();
                state.committedNext.set(s, 0);
                state.storedCount.set(s, 0);
                state.cloudHeld.set(s, 0);
            }
            state.setSession(sid, sessionId, false);
            store.event("session_start", id);
        } catch (SQLException e) {
            state.storageErrors.incrementAndGet();
            if (disk.isDiskFull(e)) {
                enterDiskFull();
            }
            state.setLastError("session switch failed: " + e.getMessage());
            needProbe = true;
            Clocks.sleepMs(500);
        }
    }

    private boolean flushPendingForSwitch() {
        flushPending();
        for (int s = 1; s <= 2; s++) {
            while (!pending[s].isEmpty()) {
                if (!convertOldestToLoss(s)) {
                    break;
                }
            }
            flushPendingUnconditionally(s);
            if (!pending[s].isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private void finishSession() {
        if (!pending[1].isEmpty() || !pending[2].isEmpty()) {
            return;
        }
        try {
            store.closeSession(sid, "session_end", List.of());
            closed = true;
            state.setSession(sid, sessionId, true);
            store.event("session_end", sessionId);
        } catch (SQLException e) {
            state.storageErrors.incrementAndGet();
            state.setLastError("close session failed: " + e.getMessage());
        }
    }

    void process(int s, MockReply rep) {
        long n0 = next[s];
        long f = rep.firstIndex();
        int count = rep.count();
        List<LossRange> known = new ArrayList<>(hints[s]);
        known.addAll(rep.lost());
        long m;
        if (count > 0) {
            m = Math.max(n0, f + count);
        } else {
            m = n0;
            boolean progressed = true;
            while (progressed) {
                progressed = false;
                for (LossRange l : known) {
                    if (l.fromIndex() <= m && l.toIndex() != null && l.toIndex() >= m) {
                        m = l.toIndex() + 1;
                        progressed = true;
                    }
                }
            }
        }
        boolean endFlag = rep.sessionEnd() && count < maxCount;
        if (m <= n0 && !(endFlag && !ended[s])) {
            hints[s] = keepFuture(known, n0);
            if (count > 0) {
                state.overlapSamplesIgnored.addAndGet(count);
            }
            return;
        }
        long gapEnd = count > 0 ? Math.min(f, m) - 1 : m - 1;
        List<LossRange> losses = cover(s, n0, gapEnd, known);
        int[][] rows = new int[0][];
        long dataFirst = m;
        if (count > 0 && f + count > n0) {
            int skip = (int) Math.max(0, n0 - f);
            rows = Arrays.copyOfRange(rep.rows(), skip, count);
            dataFirst = f + skip;
            if (skip > 0) {
                state.overlapSamplesIgnored.addAndGet(skip);
            }
        }
        next[s] = m;
        if (endFlag) {
            ended[s] = true;
        }
        hints[s] = keepFuture(known, m);
        submit(new DeviceStore.Commit(sid, s, losses, dataFirst, rows, m, endFlag));
    }

    static List<LossRange> cover(int sensor, long from, long to, List<LossRange> known) {
        List<LossRange> out = new ArrayList<>();
        if (to < from) {
            return out;
        }
        List<LossRange> sorted = new ArrayList<>(known);
        sorted.sort(Comparator.comparingLong(LossRange::fromIndex));
        long pos = from;
        while (pos <= to) {
            LossRange covering = null;
            long nextStart = Long.MAX_VALUE;
            for (LossRange l : sorted) {
                if (l.fromIndex() <= pos && l.endInclusive() >= pos) {
                    covering = l;
                    break;
                }
                if (l.fromIndex() > pos) {
                    nextStart = Math.min(nextStart, l.fromIndex());
                }
            }
            long end;
            String reason;
            if (covering != null) {
                end = Math.min(covering.endInclusive(), to);
                reason = covering.reason();
            } else {
                end = Math.min(nextStart == Long.MAX_VALUE ? to : nextStart - 1, to);
                reason = R_UNDELIVERED;
            }
            if (!out.isEmpty()) {
                LossRange prev = out.get(out.size() - 1);
                if (prev.reason().equals(reason) && prev.toIndex() == pos - 1) {
                    out.set(out.size() - 1, new LossRange(sensor, prev.fromIndex(), end, reason));
                    pos = end + 1;
                    continue;
                }
            }
            out.add(new LossRange(sensor, pos, end, reason));
            pos = end + 1;
        }
        return out;
    }

    private static List<LossRange> keepFuture(List<LossRange> known, long m) {
        List<LossRange> keep = new ArrayList<>();
        for (LossRange l : known) {
            if (l.endInclusive() >= m && keep.size() < MAX_HINTS) {
                keep.add(l);
            }
        }
        return keep;
    }

    private void submit(DeviceStore.Commit c) {
        int s = c.sensor();
        if (!pending[s].isEmpty() || state.diskFull()) {
            enqueue(c);
            return;
        }
        if (disk.low()) {
            enterDiskFull();
            enqueue(c);
            return;
        }
        try {
            long inserted = store.commit(c);
            afterCommit(c, inserted);
        } catch (SQLException e) {
            state.storageErrors.incrementAndGet();
            if (disk.isDiskFull(e)) {
                enterDiskFull();
            } else {
                state.setLastError("storage: " + e.getMessage());
            }
            enqueue(c);
        }
    }

    private void afterCommit(DeviceStore.Commit c, long inserted) {
        int s = c.sensor();
        state.committedNext.accumulateAndGet(s, c.newNext(), Math::max);
        state.storedCount.addAndGet(s, inserted);
        if (!c.losses().isEmpty()) {
            state.lossesVersion.incrementAndGet();
        }
        state.recordCommit(s);
    }

    private void enqueue(DeviceStore.Commit c) {
        int s = c.sensor();
        pending[s].addLast(c);
        pendingSamples[s] += c.rows().length;
        while (pendingSamples[s] > memoryCapSamples) {
            if (!convertOldestToLoss(s)) {
                break;
            }
        }
        state.pendingSamples.set(pendingSamples[1] + pendingSamples[2]);
    }

    private boolean convertOldestToLoss(int s) {
        DeviceStore.Commit victim = null;
        int position = 0;
        for (DeviceStore.Commit c : pending[s]) {
            if (c.rows().length > 0) {
                victim = c;
                break;
            }
            position++;
        }
        if (victim == null) {
            return false;
        }
        String reason = state.diskFull() ? R_DISK_FULL : R_STORAGE;
        long from = victim.dataFirst();
        long to = victim.dataFirst() + victim.rows().length - 1;
        List<LossRange> losses = new ArrayList<>(victim.losses());
        LossRange dropped = new LossRange(s, from, to, reason);
        ArrayDeque<DeviceStore.Commit> rebuilt = new ArrayDeque<>();
        int k = 0;
        for (DeviceStore.Commit c : pending[s]) {
            if (k == position) {
                DeviceStore.Commit prev = rebuilt.peekLast();
                if (prev != null && prev.rows().length == 0 && prev.sid() == victim.sid() && !prev.losses().isEmpty()) {
                    LossRange last = prev.losses().get(prev.losses().size() - 1);
                    if (last.reason().equals(reason) && last.toIndex() != null && last.toIndex() == from - 1 && losses.isEmpty()) {
                        List<LossRange> merged = new ArrayList<>(prev.losses());
                        merged.set(merged.size() - 1, new LossRange(s, last.fromIndex(), to, reason));
                        rebuilt.pollLast();
                        rebuilt.addLast(new DeviceStore.Commit(prev.sid(), s, merged, victim.newNext(), new int[0][],
                                victim.newNext(), victim.ended()));
                        k++;
                        continue;
                    }
                }
                losses.add(dropped);
                rebuilt.addLast(new DeviceStore.Commit(victim.sid(), s, losses, victim.newNext(),
                        new int[0][], victim.newNext(), victim.ended()));
            } else {
                rebuilt.addLast(c);
            }
            k++;
        }
        pending[s].clear();
        pending[s].addAll(rebuilt);
        pendingSamples[s] -= victim.rows().length;
        state.pendingSamples.set(pendingSamples[1] + pendingSamples[2]);
        return true;
    }

    void flushPending() {
        for (int s = 1; s <= 2; s++) {
            while (!pending[s].isEmpty()) {
                DeviceStore.Commit c = pending[s].peekFirst();
                boolean lossOnly = c.rows().length == 0;
                if (state.diskFull() && !lossOnly && !disk.recovered()) {
                    break;
                }
                if (!commitHead(s, c)) {
                    break;
                }
            }
        }
        if (state.diskFull() && pending[1].isEmpty() && pending[2].isEmpty() && disk.recovered()) {
            state.setDiskFull(false);
            disk.ensureReserve();
            store.event("disk_recovered", "free=" + disk.free());
        }
        state.pendingSamples.set(pendingSamples[1] + pendingSamples[2]);
    }

    private void flushPendingUnconditionally(int s) {
        while (!pending[s].isEmpty()) {
            if (!commitHead(s, pending[s].peekFirst())) {
                return;
            }
        }
    }

    private boolean commitHead(int s, DeviceStore.Commit c) {
        try {
            long inserted = store.commit(c);
            pending[s].pollFirst();
            pendingSamples[s] -= c.rows().length;
            afterCommit(c, inserted);
            return true;
        } catch (SQLException e) {
            state.storageErrors.incrementAndGet();
            if (disk.isDiskFull(e)) {
                enterDiskFull();
            }
            return false;
        }
    }

    private void enterDiskFull() {
        if (!state.diskFull()) {
            state.setDiskFull(true);
            disk.releaseReserve();
            store.event("disk_full", "free=" + disk.free());
        }
    }
}
