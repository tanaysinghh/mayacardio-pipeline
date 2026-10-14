package com.mayacardio.pipeline.core;

public record SampleRun(int sensor, long firstIndex, int[][] rows) {

    public int count() {
        return rows.length;
    }

    public long lastIndex() {
        return firstIndex + rows.length - 1;
    }
}
