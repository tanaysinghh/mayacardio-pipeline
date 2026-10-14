package com.mayacardio.pipeline.mock;

public final class Generator {
    public static final int RATE = 400;
    public static final long MOD = 8388608L;
    private static final double R_AMPLITUDE = 1_500_000.0;
    private static final int BASELINE = 1200;

    private final String mode;
    private final long seed;

    public Generator(String mode, long seed) {
        if (!mode.equals("signal") && !mode.equals("ramp")) {
            throw new IllegalArgumentException("mode must be signal or ramp");
        }
        this.mode = mode;
        this.seed = seed;
    }

    public String mode() {
        return mode;
    }

    public long seed() {
        return seed;
    }

    public static int channels(int sensor) {
        return sensor == 1 ? 3 : 2;
    }

    public int[] sample(int sensor, long i) {
        if (mode.equals("ramp")) {
            return ramp(sensor, i);
        }
        return sensor == 1 ? ecg(i) : slow(i);
    }

    static int[] ramp(int sensor, long i) {
        int n = channels(sensor);
        long mult = sensor == 1 ? 3 : 2;
        int[] row = new int[n];
        for (int c = 1; c <= n; c++) {
            row[c - 1] = (int) Math.floorMod(mult * i + c, MOD);
        }
        return row;
    }

    private int[] ecg(long i) {
        double phase = beatPhase(i / (double) RATE);
        double frac = phase - Math.floor(phase);
        int ch2 = BASELINE + (int) Math.round(R_AMPLITUDE * wave(frac)) + noise(1, 2, i, 300);
        int ch1 = (int) Math.round(0.6 * ch2);
        int ch3 = (int) Math.round(1.3 * ch2);
        return new int[]{ch1, ch2, ch3};
    }

    private int[] slow(long i) {
        double t = i / (double) RATE;
        int ch1 = 400_000 + (int) Math.round(20_000 * Math.sin(2 * Math.PI * t / 37.0) + 5_000 * Math.sin(2 * Math.PI * t / 11.3))
                + noise(2, 1, i, 200);
        int ch2 = 150_000 + (int) Math.round(8_000 * Math.sin(2 * Math.PI * t / 53.0 + 1.0)) + noise(2, 2, i, 200);
        return new int[]{ch1, ch2};
    }

    public static int bpmAt(double t) {
        int minute = (int) Math.floor(t / 60.0) % 6;
        return switch (minute) {
            case 0, 1 -> 60;
            case 2, 3 -> 90;
            default -> 72;
        };
    }

    static double beatPhase(double t) {
        double cycles = Math.floor(t / 360.0);
        double tc = t - cycles * 360.0;
        double within;
        if (tc < 120) {
            within = tc;
        } else if (tc < 240) {
            within = 120 + (tc - 120) * 1.5;
        } else {
            within = 300 + (tc - 240) * 1.2;
        }
        return cycles * 444 + within;
    }

    static double wave(double f) {
        return gauss(f, 0.15, 0.025, 0.12)
                + gauss(f, 0.27, 0.006, -0.10)
                + gauss(f, 0.30, 0.008, 1.0)
                + gauss(f, 0.33, 0.007, -0.20)
                + gauss(f, 0.55, 0.05, 0.30);
    }

    private static double gauss(double x, double mu, double sigma, double amp) {
        double d = (x - mu) / sigma;
        return amp * Math.exp(-0.5 * d * d);
    }

    private int noise(int sensor, int channel, long i, int amplitude) {
        long h = mix(seed * 0x9E3779B97F4A7C15L + sensor * 0xBF58476D1CE4E5B9L + channel * 0x94D049BB133111EBL + i);
        return (int) Math.floorMod(h, 2L * amplitude + 1) - amplitude;
    }

    static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
