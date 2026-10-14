package com.mayacardio.pipeline.core;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

public record WindowResult(
        long window,
        int missing1,
        int missing2,
        BigDecimal[] mean1,
        int[] min1,
        int[] max1,
        BigDecimal[] mean2,
        int[] min2,
        int[] max2,
        BigDecimal magnitude,
        BigDecimal heartRate) {

    public boolean complete() {
        return missing1 == 0 && missing2 == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WindowResult r)) {
            return false;
        }
        return window == r.window && missing1 == r.missing1 && missing2 == r.missing2
                && Arrays.equals(mean1, r.mean1) && Arrays.equals(min1, r.min1) && Arrays.equals(max1, r.max1)
                && Arrays.equals(mean2, r.mean2) && Arrays.equals(min2, r.min2) && Arrays.equals(max2, r.max2)
                && Objects.equals(magnitude, r.magnitude) && Objects.equals(heartRate, r.heartRate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(window, missing1, missing2, Arrays.hashCode(mean1), magnitude, heartRate);
    }

    @Override
    public String toString() {
        return "WindowResult{w=" + window + ", missing=" + missing1 + "/" + missing2
                + ", mean1=" + Arrays.toString(mean1) + ", min1=" + Arrays.toString(min1) + ", max1=" + Arrays.toString(max1)
                + ", mean2=" + Arrays.toString(mean2) + ", min2=" + Arrays.toString(min2) + ", max2=" + Arrays.toString(max2)
                + ", magnitude=" + magnitude + ", hr=" + heartRate + "}";
    }
}
