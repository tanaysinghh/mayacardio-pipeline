package com.mayacardio.pipeline.device;

import java.time.Instant;

public final class Clocks {

    private Clocks() {
    }

    public static long monoMs() {
        return System.nanoTime() / 1_000_000L;
    }

    public static String wallIso() {
        return Instant.now().toString();
    }

    public static long wallMs() {
        return System.currentTimeMillis();
    }

    public static void sleepMs(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
