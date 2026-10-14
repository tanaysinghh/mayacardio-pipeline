package com.mayacardio.pipeline.cloud.service;

import com.mayacardio.pipeline.cloud.store.Segment;
import com.mayacardio.pipeline.core.BatchCodec;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.SampleRun;
import com.mayacardio.pipeline.core.Signal;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class IngestService {

    public record IngestResult(String sessionId, long newSamples, long duplicates, long conflicts, long held1, long held2) {
    }

    private final SessionRegistry registry;
    private final ProcessingService processing;

    public IngestService(SessionRegistry registry, ProcessingService processing) {
        this.registry = registry;
        this.processing = processing;
    }

    public IngestResult ingest(BatchCodec.Batch batch) {
        SessionState st = registry.get(batch.sessionId());
        long fresh = 0;
        long dup = 0;
        long conflicts = 0;
        for (int sensor = 1; sensor <= 2; sensor++) {
            List<SampleRun> runs = new ArrayList<>();
            for (SampleRun r : batch.runs()) {
                if (r.sensor() == sensor && r.count() > 0) {
                    runs.add(r);
                }
            }
            if (runs.isEmpty()) {
                continue;
            }
            synchronized (st.lock(sensor)) {
                Map<Long, Segment> touched = new LinkedHashMap<>();
                for (SampleRun run : runs) {
                    Map<Long, Segment> segs = registry.segments(st, sensor,
                            Signal.windowOf(run.firstIndex()), Signal.windowOf(run.lastIndex()));
                    int[][] rows = run.rows();
                    for (int k = 0; k < rows.length; k++) {
                        long idx = run.firstIndex() + k;
                        Segment seg = segs.get(Signal.windowOf(idx));
                        switch (seg.put((int) (idx - seg.firstIndex()), rows[k])) {
                            case NEW -> {
                                fresh++;
                                touched.put(seg.seg(), seg);
                            }
                            case SAME -> dup++;
                            case CONFLICT -> conflicts++;
                        }
                    }
                }
                try {
                    registry.store().saveSegments(st.id(), sensor, touched.values());
                } catch (RuntimeException e) {
                    for (Long g : touched.keySet()) {
                        st.cache(sensor).remove(g);
                    }
                    throw e;
                }
                for (SampleRun run : runs) {
                    st.addHeld(sensor, run.firstIndex(), run.lastIndex());
                }
            }
        }
        if (conflicts > 0) {
            st.addConflicts(conflicts);
        }
        st.noteIngest(Instant.now().toString(), batch.capturedAtMillis());
        processing.signal(st);
        return new IngestResult(st.id(), fresh, dup, conflicts, st.heldCount(1), st.heldCount(2));
    }

    public int addLosses(String sessionId, List<LossRange> losses) {
        SessionState st = registry.get(sessionId);
        registry.store().insertLosses(sessionId, losses);
        for (LossRange l : losses) {
            st.addLoss(l);
        }
        processing.signal(st);
        return losses.size();
    }

    public void setMeta(String sessionId, boolean closed, Long next1, Long next2, String reason) {
        SessionState st = registry.get(sessionId);
        registry.store().setMeta(sessionId, closed, next1, next2, reason);
        st.setClosed(closed, next1, next2);
        processing.signal(st);
    }
}
