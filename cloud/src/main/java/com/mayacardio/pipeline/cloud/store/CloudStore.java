package com.mayacardio.pipeline.cloud.store;

import com.mayacardio.pipeline.core.LossRange;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface CloudStore {

    record SessionMeta(String sessionId, String createdAt, boolean closed, Long finalNext1, Long finalNext2, String closeReason) {
    }

    record SegmentSummary(long seg, byte[] data) {
    }

    record StoredResult(long window, String mode, String body) {
    }

    String backend();

    void init();

    void ensureSession(String sessionId, String createdAt);

    List<SessionMeta> sessions();

    SessionMeta meta(String sessionId);

    void setMeta(String sessionId, boolean closed, Long finalNext1, Long finalNext2, String closeReason);

    Map<Long, Segment> loadSegments(String sessionId, int sensor, Collection<Long> segs);

    void saveSegments(String sessionId, int sensor, Collection<Segment> segments);

    void forEachSegment(String sessionId, int sensor, Consumer<SegmentSummary> consumer);

    void insertLosses(String sessionId, List<LossRange> losses);

    List<LossRange> losses(String sessionId);

    void upsertResults(String sessionId, List<StoredResult> results, String computedAt);

    void forEachResult(String sessionId, String mode, Consumer<StoredResult> consumer);

    Set<Long> resultWindows(String sessionId, String mode);

    void deleteSession(String sessionId);
}
