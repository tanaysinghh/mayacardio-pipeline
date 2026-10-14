package com.mayacardio.pipeline.core;

public interface SignalView {

    boolean isResolved(int sensor, long from, long to);

    SampleBlock load(int sensor, long from, long to);
}
