package com.mayacardio.pipeline.core;

import java.util.Objects;

public record LossRange(int sensor, long fromIndex, Long toIndex, String reason) {

    public LossRange {
        Objects.requireNonNull(reason, "reason");
        if (toIndex != null && toIndex < fromIndex) {
            throw new IllegalArgumentException("to_index before from_index");
        }
    }

    public long endInclusive() {
        return toIndex == null ? Long.MAX_VALUE : toIndex;
    }

    public boolean openEnded() {
        return toIndex == null;
    }

    public long size() {
        return toIndex == null ? -1 : toIndex - fromIndex + 1;
    }
}
