package com.mayacardio.pipeline.tools;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ReferenceProcessor {
    private final ExportData data;
    private final List<Long> peaks;

    public ReferenceProcessor(ExportData data) {
        this.data = data;
        this.peaks = allPeaks();
    }

    private boolean has(int sensor, long i) {
        return i >= 0 && i <= data.maxIndex(sensor) && data.present[sensor].get((int) i);
    }

    private int ch2(long i) {
        return data.values[1][1][(int) i];
    }

    private List<Long> allPeaks() {
        List<Long> out = new ArrayList<>();
        Long last = null;
        long n = data.maxIndex(1);
        for (long i = 800; i < n; i++) {
            if (!has(1, i) || !has(1, i + 1)) {
                continue;
            }
            boolean history = true;
            int max = Integer.MIN_VALUE;
            for (long k = i - 800; k < i; k++) {
                if (!has(1, k)) {
                    history = false;
                    break;
                }
                max = Math.max(max, ch2(k));
            }
            if (!history) {
                continue;
            }
            int v = ch2(i);
            if (v > ch2(i - 1) && v > ch2(i + 1) && v > max / 2.0 && (last == null || i - last >= 100)) {
                out.add(i);
                last = i;
            }
        }
        return out;
    }

    private static String r1(double x) {
        BigDecimal r = new BigDecimal(x).setScale(1, com.mayacardio.pipeline.core.Interpretation.rounding());
        return r.signum() == 0 ? "0.0" : r.toPlainString();
    }

    public Map<String, Object> window(long w) {
        Map<String, Object> m = new LinkedHashMap<>();
        long a = 400 * w;
        long e = a + 399;
        int[] missing = new int[3];
        for (int sensor = 1; sensor <= 2; sensor++) {
            int channels = sensor == 1 ? 3 : 2;
            List<String> mean = new ArrayList<>();
            List<Integer> min = new ArrayList<>();
            List<Integer> max = new ArrayList<>();
            int count = 0;
            for (long i = a; i <= e; i++) {
                if (has(sensor, i)) {
                    count++;
                }
            }
            missing[sensor] = 400 - count;
            for (int c = 0; c < channels; c++) {
                long sum = 0;
                Integer mn = null;
                Integer mx = null;
                for (long i = a; i <= e; i++) {
                    if (has(sensor, i)) {
                        int v = data.values[sensor][c][(int) i];
                        sum += v;
                        mn = mn == null ? v : Math.min(mn, v);
                        mx = mx == null ? v : Math.max(mx, v);
                    }
                }
                if (count > 0) {
                    mean.add(r1((double) sum / count));
                    min.add(mn);
                    max.add(mx);
                }
            }
            m.put("mean_" + sensor, count == 0 ? null : mean);
            m.put("min_" + sensor, count == 0 ? null : min);
            m.put("max_" + sensor, count == 0 ? null : max);
        }
        m.put("missing_1", missing[1]);
        m.put("missing_2", missing[2]);
        m.put("complete", missing[1] == 0 && missing[2] == 0);
        double mag = 0;
        int n2 = 0;
        for (long i = a; i <= e; i++) {
            if (has(2, i)) {
                double x = data.values[2][0][(int) i];
                double y = data.values[2][1][(int) i];
                mag += Math.sqrt(x * x + y * y);
                n2++;
            }
        }
        m.put("magnitude", n2 == 0 ? null : r1(mag / n2));
        String hr = null;
        if (w >= 4) {
            long ha = 400 * (w - 4);
            boolean all = true;
            boolean both = com.mayacardio.pipeline.core.Interpretation.hrNeedsBothSensors();
            for (long i = ha; i <= e; i++) {
                if (!has(1, i) || both && !has(2, i)) {
                    all = false;
                    break;
                }
            }
            if (all) {
                List<Long> p = peaks.stream().filter(x -> x >= ha && x <= e).toList();
                if (p.size() >= 2) {
                    double sum = 0;
                    for (int k = 0; k + 1 < p.size(); k++) {
                        sum += (p.get(k + 1) - p.get(k)) / 400.0;
                    }
                    hr = r1(60.0 / (sum / (p.size() - 1)));
                }
            }
        }
        m.put("heart_rate", hr);
        return m;
    }
}
