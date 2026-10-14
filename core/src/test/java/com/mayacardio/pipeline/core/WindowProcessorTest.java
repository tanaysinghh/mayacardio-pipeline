package com.mayacardio.pipeline.core;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowProcessorTest {

    private static BigDecimal d(String s) {
        return new BigDecimal(s);
    }

    @Test
    void completeRampWindowStatistics() {
        MemorySignalView v = TestSignals.ramp(4000);
        WindowResult r = WindowProcessor.compute(v, 0);
        assertNotNull(r);
        assertEquals(0, r.missing1());
        assertEquals(0, r.missing2());
        assertTrue(r.complete());
        assertArrayEquals(new BigDecimal[]{d("599.5"), d("600.5"), d("601.5")}, r.mean1());
        assertArrayEquals(new int[]{1, 2, 3}, r.min1());
        assertArrayEquals(new int[]{1198, 1199, 1200}, r.max1());
        assertArrayEquals(new BigDecimal[]{d("400.0"), d("401.0")}, r.mean2());
        assertArrayEquals(new int[]{1, 2}, r.min2());
        assertArrayEquals(new int[]{799, 800}, r.max2());
        double mag = 0;
        for (int i = 0; i < 400; i++) {
            long a = 2L * i + 1;
            long b = 2L * i + 2;
            mag += Math.sqrt((double) (a * a + b * b));
        }
        assertEquals(Rounding.r1(mag / 400), r.magnitude());
        assertNull(r.heartRate());
    }

    @Test
    void missingSamplesAreCountedAndExcluded() {
        MemorySignalView full = TestSignals.ramp(4000);
        MemorySignalView v = new MemorySignalView();
        for (long i = 0; i < 4000; i++) {
            if (i < 400 || i > 409) {
                v.put(1, i, full.load(1, i, i).row(i));
            }
            if (i % 2 == 0) {
                v.put(2, i, full.load(2, i, i).row(i));
            }
        }
        v.setResolvedEnd(1, 4000);
        v.setResolvedEnd(2, 4000);
        WindowResult r = WindowProcessor.compute(v, 1);
        assertEquals(10, r.missing1());
        assertEquals(200, r.missing2());
        assertFalse(r.complete());
        assertArrayEquals(new int[]{3 * 410 + 1, 3 * 410 + 2, 3 * 410 + 3}, r.min1());
        assertArrayEquals(new int[]{2 * 400 + 1, 2 * 400 + 2}, r.min2());
        assertArrayEquals(new int[]{2 * 798 + 1, 2 * 798 + 2}, r.max2());
    }

    @Test
    void noSensorTwoSamplesGivesNulls() {
        MemorySignalView v = new MemorySignalView();
        for (long i = 0; i < 800; i++) {
            v.put(1, i, new int[]{1, 2, 3});
        }
        v.setResolvedEnd(1, 800);
        v.setResolvedEnd(2, 800);
        WindowResult r = WindowProcessor.compute(v, 0);
        assertEquals(0, r.missing1());
        assertEquals(400, r.missing2());
        assertNull(r.mean2());
        assertNull(r.min2());
        assertNull(r.max2());
        assertNull(r.magnitude());
        assertFalse(r.complete());
    }

    @Test
    void notReadyUntilNextSampleOfSensorOneIsResolved() {
        MemorySignalView v = TestSignals.ramp(4000);
        v.setResolvedEnd(1, 400);
        assertNull(WindowProcessor.compute(v, 0));
        v.setResolvedEnd(1, 401);
        assertNotNull(WindowProcessor.compute(v, 0));
    }

    @Test
    void sessionEndResolvesTrailingIndices() {
        MemorySignalView v = TestSignals.ramp(400);
        v.setSessionEnd(1, 400);
        v.setSessionEnd(2, 400);
        assertNotNull(WindowProcessor.compute(v, 0));
    }

    @Test
    void heartRateNullBeforeWindowFour() {
        MemorySignalView v = TestSignals.spikes(8000, 400, 1_000_000);
        for (int w = 0; w < 4; w++) {
            assertNull(WindowProcessor.compute(v, w).heartRate(), "w=" + w);
        }
        assertEquals(d("60.0"), WindowProcessor.compute(v, 4).heartRate());
    }

    @Test
    void heartRateNinetyBpm() {
        Integer[] ch = new Integer[12000];
        Arrays.fill(ch, 10);
        double t = 300;
        while (t < ch.length) {
            ch[(int) Math.round(t)] = 500_000;
            t += 400.0 * 60 / 90;
        }
        MemorySignalView v = TestSignals.fromChannel(ch);
        WindowResult r = WindowProcessor.compute(v, 20);
        double expected = expectedHr(NaivePeaks.all(ch), 20);
        assertEquals(Rounding.r1(expected), r.heartRate());
        assertEquals(d("90.0"), r.heartRate());
    }

    @Test
    void heartRateNullWhenAnySampleInFiveSecondsMissing() {
        MemorySignalView v = TestSignals.spikes(8000, 400, 1_000_000);
        MemorySignalView g = new MemorySignalView();
        for (long i = 0; i < 8000; i++) {
            if (i != 2000) {
                g.put(1, i, v.load(1, i, i).row(i));
            }
            g.put(2, i, v.load(2, i, i).row(i));
        }
        g.setResolvedEnd(1, 8000);
        g.setResolvedEnd(2, 8000);
        assertNull(WindowProcessor.compute(g, 9).heartRate());
        assertNull(WindowProcessor.compute(g, 5).heartRate());
        assertNotNull(WindowProcessor.compute(g, 10).heartRate());
        assertNotNull(WindowProcessor.compute(g, 4).heartRate());
    }

    @Test
    void heartRateNullWithFewerThanTwoPeaks() {
        Integer[] ch = new Integer[6000];
        Arrays.fill(ch, 5);
        ch[3000] = 1000;
        MemorySignalView v = TestSignals.fromChannel(ch);
        assertNull(WindowProcessor.compute(v, 8).heartRate());
    }

    @Test
    void refractoryOfOneHundredSamples() {
        Integer[] ch = new Integer[6000];
        Arrays.fill(ch, 0);
        ch[2000] = 1000;
        ch[2099] = 900;
        ch[2500] = 1000;
        ch[2600] = 900;
        MemorySignalView v = TestSignals.fromChannel(ch);
        long[] p = PeakDetector.peaks(v, 1600, 3599);
        assertArrayEquals(new long[]{2000, 2500, 2600}, p);
        assertEquals(NaivePeaks.all(ch).stream().filter(x -> x >= 1600 && x <= 3599).toList(),
                Arrays.stream(p).boxed().toList());
    }

    @Test
    void suppressedCandidateDoesNotResetRefractory() {
        Integer[] ch = new Integer[6000];
        Arrays.fill(ch, 0);
        ch[2000] = 1000;
        ch[2060] = 900;
        ch[2120] = 900;
        ch[2180] = 900;
        MemorySignalView v = TestSignals.fromChannel(ch);
        long[] p = PeakDetector.peaks(v, 2100, 3000);
        assertArrayEquals(new long[]{2120}, p);
    }

    @Test
    void mustExceedHalfOfPrecedingMaximum() {
        Integer[] ch = new Integer[6000];
        Arrays.fill(ch, 0);
        ch[2000] = 1000;
        ch[2400] = 500;
        ch[2800] = 501;
        MemorySignalView v = TestSignals.fromChannel(ch);
        assertArrayEquals(new long[]{2000, 2800}, PeakDetector.peaks(v, 1600, 3599));
    }

    @Test
    void needsEightHundredSamplesOfHistoryAndTheNextSample() {
        Integer[] ch = new Integer[6000];
        Arrays.fill(ch, 0);
        ch[799] = 1000;
        ch[1300] = 1000;
        ch[1500] = 1000;
        ch[1501] = null;
        ch[2200] = 1000;
        ch[3500] = 1000;
        MemorySignalView v = TestSignals.fromChannel(ch);
        assertArrayEquals(new long[]{1300, 3500}, PeakDetector.peaks(v, 0, 3999));
    }

    @Test
    void equalNeighbourIsNotAPeak() {
        Integer[] ch = new Integer[4000];
        Arrays.fill(ch, 0);
        ch[2000] = 1000;
        ch[2001] = 1000;
        MemorySignalView v = TestSignals.fromChannel(ch);
        assertArrayEquals(new long[0], PeakDetector.peaks(v, 1000, 3000));
    }

    @Test
    void negativeValuesUseExactHalfComparison() {
        Integer[] ch = new Integer[4000];
        Arrays.fill(ch, -1000);
        ch[2000] = -400;
        MemorySignalView v = TestSignals.fromChannel(ch);
        assertArrayEquals(new long[]{2000}, PeakDetector.peaks(v, 1000, 3000));
    }

    @Test
    void peaksNotReadyWhenHistoryUnresolved() {
        MemorySignalView v = TestSignals.spikes(8000, 400, 1_000_000);
        v.setResolvedEnd(1, 1000);
        assertNull(PeakDetector.peaks(v, 1600, 3599));
    }

    @Test
    void localAnchoredPeaksMatchGlobalScanOnRandomSignals() {
        java.util.Random rnd = new java.util.Random(42);
        for (int trial = 0; trial < 60; trial++) {
            int n = 12000;
            Integer[] ch = new Integer[n];
            int base = rnd.nextInt(2000) - 1000;
            for (int i = 0; i < n; i++) {
                ch[i] = base + rnd.nextInt(trial % 3 == 0 ? 50 : 400);
                if (rnd.nextInt(300) == 0) {
                    ch[i] += 2000 + rnd.nextInt(3000);
                }
            }
            int gaps = rnd.nextInt(4);
            for (int g = 0; g < gaps; g++) {
                int s = rnd.nextInt(n - 50);
                int len = 1 + rnd.nextInt(40);
                for (int k = s; k < s + len; k++) {
                    ch[k] = null;
                }
            }
            MemorySignalView v = TestSignals.fromChannel(ch);
            java.util.List<Long> all = NaivePeaks.all(ch);
            for (int w = 4; w < n / 400 - 1; w++) {
                long a = 400L * (w - 4);
                long b = 400L * (w + 1) - 1;
                long[] p = PeakDetector.peaks(v, a, b);
                java.util.List<Long> expected = all.stream().filter(x -> x >= a && x <= b).toList();
                assertEquals(expected, Arrays.stream(p).boxed().toList(), "trial " + trial + " window " + w);
            }
        }
    }

    @Test
    void denseCandidatesRequireLongLookback() {
        Integer[] ch = new Integer[12000];
        for (int i = 0; i < ch.length; i++) {
            ch[i] = (i % 2 == 0) ? 1000 : 0;
        }
        MemorySignalView v = TestSignals.fromChannel(ch);
        java.util.List<Long> all = NaivePeaks.all(ch);
        for (int w = 4; w < 28; w++) {
            long a = 400L * (w - 4);
            long b = 400L * (w + 1) - 1;
            long[] p = PeakDetector.peaks(v, a, b);
            assertEquals(all.stream().filter(x -> x >= a && x <= b).toList(), Arrays.stream(p).boxed().toList());
        }
    }

    private static double expectedHr(java.util.List<Long> all, int w) {
        long a = 400L * (w - 4);
        long b = 400L * (w + 1) - 1;
        java.util.List<Long> p = all.stream().filter(x -> x >= a && x <= b).toList();
        double sum = 0;
        for (int k = 0; k + 1 < p.size(); k++) {
            sum += (p.get(k + 1) - p.get(k)) / 400.0;
        }
        return 60.0 / (sum / (p.size() - 1));
    }
}
