package com.mayacardio.pipeline.core;

import java.math.BigDecimal;

public final class WindowProcessor {

    private WindowProcessor() {
    }

    public static boolean ready(SignalView view, long w) {
        long a = Signal.windowStart(w);
        long e = Signal.windowEnd(w);
        return view.isResolved(1, a, e + 1) && view.isResolved(2, a, e);
    }

    public static WindowResult compute(SignalView view, long w) {
        if (w < 0) {
            throw new IllegalArgumentException("negative window");
        }
        long a = Signal.windowStart(w);
        long e = Signal.windowEnd(w);
        if (!ready(view, w)) {
            return null;
        }
        SampleBlock s1 = view.load(1, a, e);
        SampleBlock s2 = view.load(2, a, e);
        ChannelStats st1 = ChannelStats.of(s1);
        ChannelStats st2 = ChannelStats.of(s2);
        BigDecimal magnitude = magnitude(s2);
        BigDecimal hr = null;
        if (w >= Signal.HR_WINDOWS - 1) {
            long ha = Signal.windowStart(w - (Signal.HR_WINDOWS - 1));
            if (!view.isResolved(1, ha, e + 1)) {
                return null;
            }
            boolean both = Interpretation.hrNeedsBothSensors();
            if (both && !view.isResolved(2, ha, e)) {
                return null;
            }
            SampleBlock h = view.load(1, ha, e);
            if (h.allPresent() && (!both || view.load(2, ha, e).allPresent())) {
                long[] peaks = PeakDetector.peaks(view, ha, e);
                if (peaks == null) {
                    return null;
                }
                hr = heartRate(peaks);
            }
        }
        return new WindowResult(w,
                Signal.WINDOW - st1.count, Signal.WINDOW - st2.count,
                st1.means(), st1.mins(), st1.maxs(),
                st2.means(), st2.mins(), st2.maxs(),
                magnitude, hr);
    }

    public static BigDecimal heartRate(long[] peaks) {
        if (peaks.length < 2) {
            return null;
        }
        double sum = 0;
        for (int k = 0; k + 1 < peaks.length; k++) {
            sum += (peaks[k + 1] - peaks[k]) / (double) Signal.RATE;
        }
        double meanGap = sum / (peaks.length - 1);
        return Rounding.r1(60.0 / meanGap);
    }

    static BigDecimal magnitude(SampleBlock s2) {
        double sum = 0;
        int n = 0;
        for (long i = s2.from(); i <= s2.to(); i++) {
            if (s2.has(i)) {
                long c1 = s2.value(i, 0);
                long c2 = s2.value(i, 1);
                sum += Math.sqrt((double) (c1 * c1 + c2 * c2));
                n++;
            }
        }
        return n == 0 ? null : Rounding.r1(sum / n);
    }

    static final class ChannelStats {
        final int channels;
        int count;
        final long[] sum;
        final int[] min;
        final int[] max;

        ChannelStats(int channels) {
            this.channels = channels;
            this.sum = new long[channels];
            this.min = new int[channels];
            this.max = new int[channels];
        }

        static ChannelStats of(SampleBlock block) {
            ChannelStats st = new ChannelStats(block.channels());
            for (long i = block.from(); i <= block.to(); i++) {
                if (!block.has(i)) {
                    continue;
                }
                for (int c = 0; c < st.channels; c++) {
                    int v = block.value(i, c);
                    if (st.count == 0 || v < st.min[c]) {
                        st.min[c] = v;
                    }
                    if (st.count == 0 || v > st.max[c]) {
                        st.max[c] = v;
                    }
                    st.sum[c] += v;
                }
                st.count++;
            }
            return st;
        }

        BigDecimal[] means() {
            if (count == 0) {
                return null;
            }
            BigDecimal[] m = new BigDecimal[channels];
            for (int c = 0; c < channels; c++) {
                m[c] = Rounding.r1((double) sum[c] / count);
            }
            return m;
        }

        int[] mins() {
            return count == 0 ? null : min.clone();
        }

        int[] maxs() {
            return count == 0 ? null : max.clone();
        }
    }
}
