package com.mayacardio.pipeline.core;

import java.util.ArrayList;
import java.util.List;

final class NaivePeaks {

    private NaivePeaks() {
    }

    static List<Long> all(Integer[] ch2) {
        List<Long> peaks = new ArrayList<>();
        long last = Long.MIN_VALUE;
        for (int i = 0; i < ch2.length; i++) {
            if (i < 800 || i + 1 >= ch2.length || ch2[i] == null || ch2[i + 1] == null) {
                continue;
            }
            boolean ok = true;
            long max = Long.MIN_VALUE;
            for (int k = i - 800; k < i; k++) {
                if (ch2[k] == null) {
                    ok = false;
                    break;
                }
                max = Math.max(max, ch2[k]);
            }
            if (!ok) {
                continue;
            }
            if (ch2[i] > ch2[i - 1] && ch2[i] > ch2[i + 1] && ch2[i] > max / 2.0
                    && (last == Long.MIN_VALUE || i - last >= 100)) {
                peaks.add((long) i);
                last = i;
            }
        }
        return peaks;
    }
}
