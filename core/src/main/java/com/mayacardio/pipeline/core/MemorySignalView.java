package com.mayacardio.pipeline.core;

import java.util.HashMap;
import java.util.Map;

public final class MemorySignalView implements SignalView {
    private final Map<Long, int[]>[] data;
    private final long[] resolvedEnd = new long[3];
    private final long[] sessionEnd = {Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE};

    @SuppressWarnings("unchecked")
    public MemorySignalView() {
        data = new Map[3];
        data[1] = new HashMap<>();
        data[2] = new HashMap<>();
    }

    public void put(int sensor, long idx, int[] row) {
        data[sensor].put(idx, row);
    }

    public void setResolvedEnd(int sensor, long endExclusive) {
        resolvedEnd[sensor] = endExclusive;
    }

    public void setSessionEnd(int sensor, long endExclusive) {
        sessionEnd[sensor] = endExclusive;
    }

    public static boolean prefixResolved(long resolvedEnd, long sessionEnd, long from, long to) {
        return resolvedEnd >= sessionEnd || to < resolvedEnd || from >= sessionEnd;
    }

    @Override
    public boolean isResolved(int sensor, long from, long to) {
        return prefixResolved(resolvedEnd[sensor], sessionEnd[sensor], from, to);
    }

    @Override
    public SampleBlock load(int sensor, long from, long to) {
        SampleBlock block = new SampleBlock(from, to, Signal.channels(sensor));
        for (long i = from; i <= to; i++) {
            int[] row = data[sensor].get(i);
            if (row != null) {
                block.put(i, row);
            }
        }
        return block;
    }
}
