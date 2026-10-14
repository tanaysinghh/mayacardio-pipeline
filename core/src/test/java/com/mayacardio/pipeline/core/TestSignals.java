package com.mayacardio.pipeline.core;

final class TestSignals {

    private TestSignals() {
    }

    static MemorySignalView ramp(long n) {
        MemorySignalView v = new MemorySignalView();
        for (long i = 0; i < n; i++) {
            v.put(1, i, new int[]{(int) ((3 * i + 1) % 8388608), (int) ((3 * i + 2) % 8388608), (int) ((3 * i + 3) % 8388608)});
            v.put(2, i, new int[]{(int) ((2 * i + 1) % 8388608), (int) ((2 * i + 2) % 8388608)});
        }
        v.setResolvedEnd(1, n);
        v.setResolvedEnd(2, n);
        return v;
    }

    static MemorySignalView spikes(long n, int period, int amplitude) {
        MemorySignalView v = new MemorySignalView();
        for (long i = 0; i < n; i++) {
            int x = (i % period == period / 2) ? amplitude : (int) (i % 7);
            v.put(1, i, new int[]{x, x, x});
            v.put(2, i, new int[]{400000, 150000});
        }
        v.setResolvedEnd(1, n);
        v.setResolvedEnd(2, n);
        return v;
    }

    static MemorySignalView fromChannel(Integer[] ch2) {
        MemorySignalView v = new MemorySignalView();
        for (int i = 0; i < ch2.length; i++) {
            if (ch2[i] != null) {
                v.put(1, i, new int[]{0, ch2[i], 0});
            }
            v.put(2, i, new int[]{1, 1});
        }
        v.setResolvedEnd(1, ch2.length);
        v.setResolvedEnd(2, ch2.length);
        return v;
    }
}
