package com.mayacardio.pipeline.core;

import java.util.ArrayDeque;
import java.util.Arrays;

public final class PeakDetector {
    public static final int SENSOR = 1;
    public static final int CHANNEL = 1;
    public static final int LOOKBACK = 800;
    public static final int REFRACTORY = 100;
    private static final long INITIAL_SPAN = 512;

    private PeakDetector() {
    }

    public static long[] peaks(SignalView view, long a, long b) {
        if (b < a) {
            return new long[0];
        }
        long span = INITIAL_SPAN;
        while (true) {
            long s = Math.max(0, a - span);
            long evalFrom = Math.max(0, s - (REFRACTORY - 1));
            long loadFrom = Math.max(0, evalFrom - LOOKBACK);
            long loadTo = b + 1;
            if (!view.isResolved(SENSOR, loadFrom, loadTo)) {
                return null;
            }
            SampleBlock block = view.load(SENSOR, loadFrom, loadTo);
            boolean[] cand = candidates(block, loadFrom, evalFrom, b);
            long start = anchor(cand, evalFrom, s, a);
            if (start >= 0) {
                return greedy(cand, evalFrom, start, a, b);
            }
            if (s == 0) {
                throw new IllegalStateException("no anchor found from stream start");
            }
            span *= 2;
        }
    }

    static boolean[] candidates(SampleBlock block, long loadFrom, long evalFrom, long b) {
        boolean[] cand = new boolean[(int) (b - evalFrom + 1)];
        ArrayDeque<Long> deque = new ArrayDeque<>();
        long lastMissing = loadFrom - 1;
        for (long j = loadFrom; j <= b; j++) {
            if (j >= evalFrom) {
                while (!deque.isEmpty() && deque.peekFirst() < j - LOOKBACK) {
                    deque.pollFirst();
                }
                if (j >= LOOKBACK && j - LOOKBACK >= loadFrom && lastMissing < j - LOOKBACK
                        && block.has(j) && block.has(j + 1)) {
                    long v = block.value(j, CHANNEL);
                    if (v > block.value(j - 1, CHANNEL) && v > block.value(j + 1, CHANNEL)) {
                        long max = block.value(deque.peekFirst(), CHANNEL);
                        if (2 * v > max) {
                            cand[(int) (j - evalFrom)] = true;
                        }
                    }
                }
            }
            if (block.has(j)) {
                int v = block.value(j, CHANNEL);
                while (!deque.isEmpty() && block.value(deque.peekLast(), CHANNEL) <= v) {
                    deque.pollLast();
                }
                deque.addLast(j);
            } else {
                lastMissing = j;
            }
        }
        return cand;
    }

    static long anchor(boolean[] cand, long evalFrom, long s, long a) {
        long lastCand = Long.MIN_VALUE;
        long best = -1;
        for (long p = evalFrom; p <= a; p++) {
            if (p >= s && (lastCand == Long.MIN_VALUE || lastCand < p - (REFRACTORY - 1))) {
                best = p;
            }
            if (cand[(int) (p - evalFrom)]) {
                lastCand = p;
            }
        }
        return best;
    }

    static long[] greedy(boolean[] cand, long evalFrom, long start, long a, long b) {
        long[] out = new long[16];
        int n = 0;
        long last = Long.MIN_VALUE;
        for (long i = start; i <= b; i++) {
            if (cand[(int) (i - evalFrom)] && (last == Long.MIN_VALUE || i - last >= REFRACTORY)) {
                last = i;
                if (i >= a) {
                    if (n == out.length) {
                        out = Arrays.copyOf(out, n * 2);
                    }
                    out[n++] = i;
                }
            }
        }
        return Arrays.copyOf(out, n);
    }
}
