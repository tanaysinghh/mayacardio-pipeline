package com.mayacardio.pipeline.core;

import java.math.RoundingMode;
import java.util.Locale;

public final class Interpretation {
    public static final String ROUNDING = "interpret.rounding";
    public static final String HR_MISSING = "interpret.hr.missing";
    public static final String RESULTS_LAYOUT = "interpret.results.layout";
    public static final String EMPTY_WINDOWS = "interpret.empty.windows";
    public static final String PARTIAL_FINAL_WINDOW = "interpret.partial.final.window";
    public static final String[] KEYS = {ROUNDING, HR_MISSING, RESULTS_LAYOUT, EMPTY_WINDOWS, PARTIAL_FINAL_WINDOW};

    private Interpretation() {
    }

    static String value(String key, String def) {
        String v = System.getProperty("pipeline." + key);
        if (v == null) {
            v = System.getenv("PIPELINE_" + key.toUpperCase(Locale.ROOT).replace('.', '_'));
        }
        return v == null || v.isBlank() ? def : v.trim().toLowerCase(Locale.ROOT);
    }

    public static RoundingMode rounding() {
        return switch (value(ROUNDING, "half_even")) {
            case "half_even" -> RoundingMode.HALF_EVEN;
            case "half_up" -> RoundingMode.HALF_UP;
            default -> throw new IllegalArgumentException(ROUNDING + " must be half_even or half_up");
        };
    }

    public static boolean hrNeedsBothSensors() {
        return switch (value(HR_MISSING, "sensor1")) {
            case "sensor1" -> false;
            case "both" -> true;
            default -> throw new IllegalArgumentException(HR_MISSING + " must be sensor1 or both");
        };
    }

    public static boolean nestedLayout() {
        return switch (value(RESULTS_LAYOUT, "flat")) {
            case "flat" -> false;
            case "nested" -> true;
            default -> throw new IllegalArgumentException(RESULTS_LAYOUT + " must be flat or nested");
        };
    }

    public static boolean skipEmptyWindows() {
        return switch (value(EMPTY_WINDOWS, "report")) {
            case "report" -> false;
            case "skip" -> true;
            default -> throw new IllegalArgumentException(EMPTY_WINDOWS + " must be report or skip");
        };
    }

    public static boolean skipPartialFinalWindow() {
        return switch (value(PARTIAL_FINAL_WINDOW, "report")) {
            case "report" -> false;
            case "skip" -> true;
            default -> throw new IllegalArgumentException(PARTIAL_FINAL_WINDOW + " must be report or skip");
        };
    }

    public static boolean suppress(WindowResult r, long sessionEndExclusive) {
        if (skipEmptyWindows() && r.missing1() == Signal.WINDOW && r.missing2() == Signal.WINDOW) {
            return true;
        }
        return skipPartialFinalWindow() && Signal.windowEnd(r.window()) >= sessionEndExclusive;
    }

    public static String describe() {
        return value(ROUNDING, "half_even") + "," + value(HR_MISSING, "sensor1") + "," + value(RESULTS_LAYOUT, "flat")
                + "," + value(EMPTY_WINDOWS, "report") + "," + value(PARTIAL_FINAL_WINDOW, "report");
    }
}
