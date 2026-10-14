package com.mayacardio.pipeline.core;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class Rounding {
    public static final RoundingMode MODE = RoundingMode.HALF_EVEN;

    private Rounding() {
    }

    public static BigDecimal r1(double x) {
        if (Double.isNaN(x) || Double.isInfinite(x)) {
            throw new IllegalArgumentException("not finite: " + x);
        }
        BigDecimal r = new BigDecimal(x).setScale(1, MODE);
        if (r.signum() == 0) {
            return BigDecimal.ZERO.setScale(1);
        }
        return r;
    }
}
