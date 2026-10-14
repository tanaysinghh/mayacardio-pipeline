package com.mayacardio.pipeline.core;

public final class Signal {
    public static final int RATE = 400;
    public static final int WINDOW = 400;
    public static final int MIN_VALUE = -8388608;
    public static final int MAX_VALUE = 8388607;
    public static final int SENSOR_COUNT = 2;
    public static final int HR_WINDOWS = 5;

    private Signal() {
    }

    public static int channels(int sensor) {
        if (sensor == 1) {
            return 3;
        }
        if (sensor == 2) {
            return 2;
        }
        throw new IllegalArgumentException("unknown sensor " + sensor);
    }

    public static boolean validSensor(int sensor) {
        return sensor == 1 || sensor == 2;
    }

    public static boolean validValue(long v) {
        return v >= MIN_VALUE && v <= MAX_VALUE;
    }

    public static long windowStart(long w) {
        return w * WINDOW;
    }

    public static long windowEnd(long w) {
        return w * WINDOW + WINDOW - 1;
    }

    public static long windowOf(long index) {
        return Math.floorDiv(index, WINDOW);
    }
}
