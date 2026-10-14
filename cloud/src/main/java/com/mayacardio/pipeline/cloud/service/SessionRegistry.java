package com.mayacardio.pipeline.cloud.service;

import com.mayacardio.pipeline.cloud.store.CloudStore;
import com.mayacardio.pipeline.cloud.store.Segment;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.ResultJson;
import com.mayacardio.pipeline.core.SampleBlock;
import com.mayacardio.pipeline.core.Signal;
import com.mayacardio.pipeline.core.SignalView;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class SessionRegistry {
    private final CloudStore store;
    private final Map<String, SessionState> sessions = new ConcurrentHashMap<>();

    public SessionRegistry(CloudStore store) {
        this.store = store;
    }

    public CloudStore store() {
        return store;
    }

    public SessionState get(String sessionId) {
        return sessions.computeIfAbsent(sessionId, this::load);
    }

    public SessionState existing(String sessionId) {
        SessionState st = sessions.get(sessionId);
        if (st != null) {
            return st;
        }
        if (store.meta(sessionId) == null) {
            return null;
        }
        return get(sessionId);
    }

    public void forget(String sessionId) {
        sessions.remove(sessionId);
    }

    private SessionState load(String sessionId) {
        store.ensureSession(sessionId, Instant.now().toString());
        SessionState st = new SessionState(sessionId);
        for (int s = 1; s <= 2; s++) {
            int sensor = s;
            store.forEachSegment(sessionId, sensor, summary -> {
                Segment seg = Segment.decode(summary.seg(), summary.data());
                long base = seg.firstIndex();
                long runStart = -1;
                for (int i = 0; i <= Segment.SIZE; i++) {
                    boolean has = i < Segment.SIZE && seg.has(i);
                    if (has && runStart < 0) {
                        runStart = base + i;
                    } else if (!has && runStart >= 0) {
                        st.addHeld(sensor, runStart, base + i - 1);
                        runStart = -1;
                    }
                }
            });
        }
        for (LossRange l : store.losses(sessionId)) {
            st.addLoss(l);
        }
        CloudStore.SessionMeta meta = store.meta(sessionId);
        if (meta != null && meta.closed()) {
            st.setClosed(true, meta.finalNext1(), meta.finalNext2());
        }
        st.initComputed(store.resultWindows(sessionId, ResultJson.ONLINE));
        return st;
    }

    public Map<Long, Segment> segments(SessionState st, int sensor, long fromSeg, long toSeg) {
        synchronized (st.lock(sensor)) {
            Map<Long, Segment> cache = st.cache(sensor);
            List<Long> missing = new ArrayList<>();
            Map<Long, Segment> out = new java.util.HashMap<>();
            for (long g = fromSeg; g <= toSeg; g++) {
                Segment seg = cache.get(g);
                if (seg != null) {
                    out.put(g, seg);
                } else {
                    missing.add(g);
                }
            }
            if (!missing.isEmpty()) {
                Map<Long, Segment> loaded = store.loadSegments(st.id(), sensor, missing);
                for (long g : missing) {
                    Segment seg = loaded.get(g);
                    if (seg == null) {
                        seg = new Segment(g, Signal.channels(sensor));
                    }
                    cache.put(g, seg);
                    out.put(g, seg);
                }
            }
            return out;
        }
    }

    public SignalView view(SessionState st) {
        return new SignalView() {
            @Override
            public boolean isResolved(int sensor, long from, long to) {
                return st.isResolved(sensor, from, to);
            }

            @Override
            public SampleBlock load(int sensor, long from, long to) {
                SampleBlock block = new SampleBlock(from, to, Signal.channels(sensor));
                if (to < from) {
                    return block;
                }
                Map<Long, Segment> segs = segments(st, sensor, Signal.windowOf(from), Signal.windowOf(to));
                synchronized (st.lock(sensor)) {
                    for (long i = from; i <= to; i++) {
                        Segment seg = segs.get(Signal.windowOf(i));
                        if (seg != null) {
                            int off = (int) (i - seg.firstIndex());
                            if (seg.has(off)) {
                                block.put(i, seg.row(off));
                            }
                        }
                    }
                }
                return block;
            }
        };
    }
}
